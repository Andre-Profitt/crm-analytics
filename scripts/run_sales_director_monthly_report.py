#!/usr/bin/env python3
"""Run the Sales Directors monthly pipeline report deck end to end."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from merge_sales_director_overlay import (
    infer_commentary_provenance,
    infer_finance_provenance,
    load_csv_rows,
    load_json,
    merge_finance_overlay_payload,
    merge_overlay_payload,
)
from md1_presets import (
    find_md1_preset,
    load_md1_preset_config,
    md1_preset_config_summary,
    md1_preset_summary,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = REPO_ROOT / "output" / "sales_director_monthly_deck_2026-03-31"
SNAPSHOT_SCRIPT = WORKSPACE / "refresh_sales_director_monthly_snapshot.py"
BUILD_SCRIPT = WORKSPACE / "build_sales_director_monthly_deck.js"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "sales_director_monthly_runs"
VALIDATION_SCRIPT = REPO_ROOT / "scripts" / "run_sales_director_deck_validation.sh"
SLIDES_VENV_PY = REPO_ROOT / ".venv_slides" / "bin" / "python"
RENDER_SCRIPT = WORKSPACE / "scripts" / "render_slides.py"
MONTAGE_SCRIPT = WORKSPACE / "scripts" / "create_montage.py"
POWERPOINT_EXPORT_SCRIPT = REPO_ROOT / "scripts" / "export_powerpoint_pdf.py"
POWERPOINT_APP = Path("/Applications/Microsoft PowerPoint.app")
DEFAULT_MD1_PRESET_CONFIG = REPO_ROOT / "config" / "sales_director_md1_presets.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Snapshot date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--md1-preset-name",
        default=None,
        help="Optional MD-1 preset name to stamp into the run metadata.",
    )
    parser.add_argument(
        "--md1-preset-config",
        default=str(DEFAULT_MD1_PRESET_CONFIG),
        help=f"Optional MD-1 preset config path (default: {DEFAULT_MD1_PRESET_CONFIG}).",
    )
    parser.add_argument(
        "--deck-title",
        default="Sales Director Monthly Pipeline Insights",
        help="Deck title override.",
    )
    parser.add_argument(
        "--deck-subtitle",
        default=(
            "Monthly leadership view of quarter coverage, renewals, churn, "
            "and slipped-deal risk."
        ),
        help="Deck subtitle override.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional explicit run directory. Defaults to output/sales_director_monthly_runs/<timestamp>_<date>.",
    )
    parser.add_argument(
        "--overlay-json",
        default=None,
        help="Optional JSON file with Finance churn and slipped-deal commentary overlays.",
    )
    parser.add_argument(
        "--commentary-csv",
        default=None,
        help="Optional filled owner commentary CSV to merge into the overlay automatically.",
    )
    parser.add_argument(
        "--finance-csv",
        default=None,
        help="Optional filled Finance churn CSV to merge into the overlay automatically.",
    )
    parser.add_argument(
        "--base-overlay",
        default=None,
        help="Optional base overlay JSON used when merging Finance/commentary CSV inputs. Defaults to --overlay-json if provided, else a blank pending overlay.",
    )
    parser.add_argument(
        "--commentary-summary-note",
        default="Owner commentary collected for current-quarter slipped deals.",
        help="Summary note to stamp onto the slipped commentary overlay when merging a CSV.",
    )
    parser.add_argument(
        "--commentary-provenance",
        choices=["auto", "external", "example"],
        default="auto",
        help="Provenance to stamp on a merged commentary CSV. Defaults to auto, which marks sample files as example.",
    )
    parser.add_argument(
        "--finance-provenance",
        choices=["auto", "external", "example"],
        default="auto",
        help="Provenance to stamp on a merged Finance CSV. Defaults to auto, which marks sample files as example.",
    )
    parser.add_argument(
        "--skip-thumbnail",
        action="store_true",
        help="Skip Quick Look thumbnail generation.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip LibreOffice/render validation bundle generation.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the final manifest as JSON.",
    )
    return parser.parse_args()


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def write_json(path: Path, value: Any) -> None:
    write_text(path, json.dumps(value, indent=2) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def build_md1_preset_context(
    *,
    preset_config_path: str,
    preset_name: str | None,
) -> dict[str, Any] | None:
    config_path = Path(preset_config_path).resolve()
    if preset_name is None and config_path == DEFAULT_MD1_PRESET_CONFIG.resolve():
        return None

    preset_config = load_md1_preset_config(config_path)
    selected_preset = None
    if preset_name:
        selected_preset = find_md1_preset(preset_config, preset_name)
        if selected_preset is None:
            available = ", ".join(preset.name for preset in preset_config.presets)
            raise RuntimeError(
                f"MD-1 preset not found: {preset_name}. Available presets: {available}"
            )

    return {
        "preset_config": md1_preset_config_summary(preset_config),
        "selected_preset_name": preset_name,
        "selected_preset": md1_preset_summary(selected_preset) if selected_preset else None,
    }


def as_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return 0


def extract_quarterly_pipeline_disclosure(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    display = ((snapshot.get("quarterly_pipeline_display") or {}).get("display_quarter") or {})
    reason = str(display.get("reason") or "").strip()
    if not reason:
        return None
    return {
        "display_reason": reason,
        "quarterly_pipeline_title": str(display.get("title") or "").strip() or None,
        "quarterly_pipeline_footnote": str(display.get("footnote") or "").strip() or None,
        "active_deal_count": as_int(display.get("active_deal_count")),
    }


def blank_overlay_payload() -> dict[str, Any]:
    return {
        "finance_churn": {
            "status": "pending",
            "provenance": "pending",
            "owner": "",
            "source_name": "",
            "headline": "",
            "summary_note": "",
            "top_accounts": [],
        },
        "slipped_commentary": {
            "status": "pending",
            "provenance": "pending",
            "summary_note": "",
            "root_cause_bullets": [],
            "owner_comments": [],
        },
    }


def prepare_overlay_input(
    *,
    run_dir: Path,
    overlay_json: str | None,
    commentary_csv: str | None,
    finance_csv: str | None,
    base_overlay: str | None,
    commentary_summary_note: str,
    commentary_provenance: str,
    finance_provenance: str,
) -> dict[str, Any]:
    requested_overlay_path = str(Path(overlay_json).resolve()) if overlay_json else None
    commentary_csv_path = str(Path(commentary_csv).resolve()) if commentary_csv else None
    finance_csv_path = str(Path(finance_csv).resolve()) if finance_csv else None
    base_overlay_path = str(Path(base_overlay).resolve()) if base_overlay else None

    if not commentary_csv and not finance_csv:
        return {
            "overlay_path": requested_overlay_path,
            "requested_overlay_path": requested_overlay_path,
            "base_overlay_path": base_overlay_path,
            "commentary_csv_path": commentary_csv_path,
            "finance_csv_path": finance_csv_path,
            "merged_overlay_path": None,
            "owner_comment_count": 0,
            "finance_top_account_count": 0,
        }

    if base_overlay_path:
        overlay = load_json(Path(base_overlay_path))
    elif requested_overlay_path:
        overlay = load_json(Path(requested_overlay_path))
        base_overlay_path = requested_overlay_path
    else:
        overlay = blank_overlay_payload()

    owner_comment_count = 0
    finance_top_account_count = 0
    merged_overlay = overlay
    if finance_csv_path:
        finance_rows = load_csv_rows(Path(finance_csv_path))
        merged_overlay, finance_top_account_count = merge_finance_overlay_payload(
            overlay=merged_overlay,
            csv_rows=finance_rows,
            finance_provenance=infer_finance_provenance(
                requested=finance_provenance,
                finance_csv_path=Path(finance_csv_path),
            ),
        )
    if commentary_csv_path:
        commentary_rows = load_csv_rows(Path(commentary_csv_path))
        merged_overlay, owner_comment_count = merge_overlay_payload(
            overlay=merged_overlay,
            csv_rows=commentary_rows,
            summary_note=commentary_summary_note,
            commentary_provenance=infer_commentary_provenance(
                requested=commentary_provenance,
                commentary_csv_path=Path(commentary_csv_path),
            ),
        )
    merged_overlay_path = run_dir / "report1_overlay.inputs_merged.json"
    write_json(merged_overlay_path, merged_overlay)
    return {
        "overlay_path": str(merged_overlay_path),
        "requested_overlay_path": requested_overlay_path,
        "base_overlay_path": base_overlay_path,
        "commentary_csv_path": commentary_csv_path,
        "finance_csv_path": finance_csv_path,
        "merged_overlay_path": str(merged_overlay_path),
        "owner_comment_count": owner_comment_count,
        "finance_top_account_count": finance_top_account_count,
    }


def run_command(*, command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
    )


def parse_json_from_stdout(stdout: str) -> dict[str, Any] | None:
    stripped = stdout.strip()
    if not stripped:
        return None
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        pass
    else:
        return payload if isinstance(payload, dict) else None

    lines = stripped.splitlines()
    for idx in range(len(lines)):
        candidate = "\n".join(lines[idx:]).strip()
        if not candidate.startswith("{"):
            continue
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def require_ok(proc: subprocess.CompletedProcess[str], *, label: str) -> dict[str, Any] | None:
    if proc.returncode != 0:
        raise RuntimeError(
            f"{label} failed with exit code {proc.returncode}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return parse_json_from_stdout(proc.stdout)


def maybe_make_thumbnail(*, deck_path: Path, run_dir: Path) -> str | None:
    thumb_dir = run_dir / "ql_thumb"
    thumb_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [
            "qlmanage",
            "-t",
            "-s",
            "2000",
            "-o",
            str(thumb_dir),
            str(deck_path),
        ],
        cwd=run_dir,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None
    png_path = thumb_dir / f"{deck_path.name}.png"
    return str(png_path) if png_path.exists() else None


def maybe_validate_deck(*, deck_path: Path, run_dir: Path, skip_validation: bool) -> dict[str, Any]:
    validation_dir = run_dir / "validation"
    if skip_validation:
        return {
            "status": "skipped",
            "detail": "Validation skipped by flag.",
            "validation_dir": str(validation_dir),
            "rendered_dir": str(validation_dir / "rendered"),
            "montage_path": str(validation_dir / "montage.png"),
            "font_report_path": str(validation_dir / "font_report.json"),
        }
    if not VALIDATION_SCRIPT.exists():
        return {
            "status": "unavailable",
            "detail": f"Validation helper not found at {VALIDATION_SCRIPT}.",
            "validation_dir": str(validation_dir),
            "rendered_dir": str(validation_dir / "rendered"),
            "montage_path": str(validation_dir / "montage.png"),
            "font_report_path": str(validation_dir / "font_report.json"),
        }

    proc = subprocess.run(
        [str(VALIDATION_SCRIPT), str(deck_path), str(validation_dir)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    rendered_dir = validation_dir / "rendered"
    montage_path = validation_dir / "montage.png"
    font_report_path = validation_dir / "font_report.json"
    if proc.returncode != 0:
        return {
            "status": "error",
            "detail": proc.stderr.strip() or proc.stdout.strip() or "Validation helper failed.",
            "validation_dir": str(validation_dir),
            "rendered_dir": str(rendered_dir),
            "montage_path": str(montage_path),
            "font_report_path": str(font_report_path),
        }
    return {
        "status": "ok",
        "detail": "Render, montage, overflow, and font checks passed.",
        "validation_dir": str(validation_dir),
        "rendered_dir": str(rendered_dir),
        "montage_path": str(montage_path) if montage_path.exists() else None,
        "font_report_path": str(font_report_path) if font_report_path.exists() else None,
    }


def maybe_generate_powerpoint_review(*, deck_path: Path, run_dir: Path) -> dict[str, Any]:
    review_dir = run_dir / "powerpoint_review"
    pdf_path = review_dir / f"{deck_path.stem}.pdf"
    rendered_dir = review_dir / "rendered"
    montage_path = review_dir / "montage.png"

    base = {
        "review_dir": str(review_dir),
        "pdf_path": str(pdf_path),
        "rendered_dir": str(rendered_dir),
        "montage_path": str(montage_path),
    }
    if not POWERPOINT_APP.exists():
        return {
            "status": "unavailable",
            "detail": f"Microsoft PowerPoint is not installed at {POWERPOINT_APP}.",
            **base,
        }
    if not POWERPOINT_EXPORT_SCRIPT.exists():
        return {
            "status": "unavailable",
            "detail": f"PowerPoint export helper not found at {POWERPOINT_EXPORT_SCRIPT}.",
            **base,
        }

    review_dir.mkdir(parents=True, exist_ok=True)
    try:
        export_proc = subprocess.run(
            [
                sys.executable,
                str(POWERPOINT_EXPORT_SCRIPT),
                "--input",
                str(deck_path),
                "--output",
                str(pdf_path),
                "--json",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=75,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "detail": "PowerPoint PDF export timed out before the review bundle could be created.",
            **base,
        }
    if export_proc.returncode != 0:
        return {
            "status": "error",
            "detail": export_proc.stderr.strip() or export_proc.stdout.strip() or "PowerPoint PDF export failed.",
            **base,
        }

    if not (SLIDES_VENV_PY.exists() and RENDER_SCRIPT.exists() and MONTAGE_SCRIPT.exists()):
        return {
            "status": "pdf_only",
            "detail": "PowerPoint PDF exported, but the PDF render helpers are unavailable.",
            **base,
        }

    script_dir = str(RENDER_SCRIPT.parent)
    pythonpath = script_dir
    if os.environ.get("PYTHONPATH"):
        pythonpath = f"{script_dir}:{os.environ['PYTHONPATH']}"
    render_proc = subprocess.run(
        [str(SLIDES_VENV_PY), str(RENDER_SCRIPT), str(pdf_path), "--output_dir", str(rendered_dir)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": pythonpath},
    )
    if render_proc.returncode != 0:
        return {
            "status": "pdf_only",
            "detail": render_proc.stderr.strip() or render_proc.stdout.strip() or "PowerPoint PDF exported, but PDF rasterization failed.",
            **base,
        }

    montage_proc = subprocess.run(
        [
            str(SLIDES_VENV_PY),
            str(MONTAGE_SCRIPT),
            "--input_dir",
            str(rendered_dir),
            "--output_file",
            str(montage_path),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": pythonpath},
    )
    if montage_proc.returncode != 0:
        return {
            "status": "pdf_only",
            "detail": montage_proc.stderr.strip() or montage_proc.stdout.strip() or "PowerPoint PDF exported, but montage generation failed.",
            **base,
        }

    return {
        "status": "ok",
        "detail": "PowerPoint PDF export and montage generation passed.",
        **base,
    }


def build_run_summary(*, manifest: dict[str, Any]) -> str:
    summary = manifest.get("deck_summary") or {}
    publish_checklist = manifest.get("publish_checklist") or {}
    validation_summary = manifest.get("validation_summary") or {}
    powerpoint_review_summary = manifest.get("powerpoint_review_summary") or {}
    md1_context = manifest.get("md1_preset_context") or {}
    lines = [
        "# Sales Directors Monthly Report Run",
        "",
        f"- Snapshot date: `{manifest.get('snapshot_date')}`",
        f"- Quarter focus: `{manifest.get('quarter_focus')}`",
        f"- Deck output: `{manifest.get('deck_path')}`",
        f"- Snapshot artifact: `{manifest.get('snapshot_path')}`",
        f"- Deck summary: `{manifest.get('deck_summary_path')}`",
    ]
    if manifest.get("overlay_path"):
        lines.append(f"- Overlay input: `{manifest['overlay_path']}`")
    if manifest.get("finance_csv_input_path"):
        lines.append(f"- Finance CSV input: `{manifest['finance_csv_input_path']}`")
    if manifest.get("owner_commentary_markdown_path"):
        lines.append(f"- Commentary request: `{manifest['owner_commentary_markdown_path']}`")
    if manifest.get("owner_commentary_csv_path"):
        lines.append(f"- Commentary CSV: `{manifest['owner_commentary_csv_path']}`")
    if manifest.get("owner_commentary_email_path"):
        lines.append(f"- Commentary email draft: `{manifest['owner_commentary_email_path']}`")
    if manifest.get("finance_churn_request_markdown_path"):
        lines.append(f"- Finance churn request: `{manifest['finance_churn_request_markdown_path']}`")
    if manifest.get("finance_churn_request_csv_path"):
        lines.append(f"- Finance churn CSV: `{manifest['finance_churn_request_csv_path']}`")
    if manifest.get("finance_churn_request_email_path"):
        lines.append(f"- Finance churn email draft: `{manifest['finance_churn_request_email_path']}`")
    if manifest.get("owner_commentary_owner_summary_markdown_path"):
        lines.append(
            f"- Commentary owner summary: `{manifest['owner_commentary_owner_summary_markdown_path']}`"
        )
    if manifest.get("owner_commentary_owner_send_list_path"):
        lines.append(
            f"- Commentary owner send list: `{manifest['owner_commentary_owner_send_list_path']}`"
        )
    if manifest.get("owner_commentary_owner_packet_index_markdown_path"):
        lines.append(
            f"- Commentary owner packet index: `{manifest['owner_commentary_owner_packet_index_markdown_path']}`"
        )
    if manifest.get("overlay_fill_template_path"):
        lines.append(f"- Overlay fill template: `{manifest['overlay_fill_template_path']}`")
    if manifest.get("approval_rule_markdown_path"):
        lines.append(f"- Approval rule contract: `{manifest['approval_rule_markdown_path']}`")
    if manifest.get("publish_checklist_markdown_path"):
        lines.append(f"- Publish checklist: `{manifest['publish_checklist_markdown_path']}`")
    if manifest.get("thumbnail_path"):
        lines.append(f"- Thumbnail: `{manifest['thumbnail_path']}`")
    if md1_context:
        lines.append(f"- MD-1 preset config: `{(md1_context.get('preset_config') or {}).get('config_path')}`")
        if md1_context.get("selected_preset"):
            selected = md1_context["selected_preset"]
            lines.append(
                f"- MD-1 preset: `{selected.get('name')}` / `{selected.get('territory')}`"
            )
    if validation_summary.get("validation_dir"):
        lines.append(f"- Validation bundle: `{validation_summary['validation_dir']}`")
    if powerpoint_review_summary.get("review_dir"):
        lines.append(f"- PowerPoint review bundle: `{powerpoint_review_summary['review_dir']}`")
    if manifest.get("internal_review_packet_markdown_path"):
        lines.append(f"- Internal review packet: `{manifest['internal_review_packet_markdown_path']}`")
    lines.extend(
        [
            "",
            "## Readout",
            "",
            f"- Biggest coverage gap: `{summary.get('biggest_gap_region')}` at `{summary.get('biggest_gap_arr')}`",
            f"- Weakest confidence region: `{summary.get('weakest_confidence_region')}` at `{summary.get('weakest_confidence_pct')}`",
            f"- Approval candidate count: `{summary.get('approval_candidate_count')}`",
            f"- Quarter renewal pipeline ACV: `{summary.get('total_open_renewal_pipeline_acv')}`",
            f"- Critical renewal ACV: `{summary.get('critical_renewal_acv')}`",
            f"- Biggest slipped region: `{summary.get('biggest_slipped_region')}` at `{summary.get('biggest_slipped_arr')}`",
            f"- Value methodology: `{summary.get('value_methodology')}`",
            f"- Finance churn overlay: `{summary.get('finance_churn_status')}`",
            f"- Slipped commentary overlay: `{summary.get('slipped_commentary_status')}`",
            f"- Publish status: `{summary.get('publish_status')}`",
            f"- Validation status: `{validation_summary.get('status') or 'n/a'}`",
            f"- PowerPoint review status: `{powerpoint_review_summary.get('status') or 'n/a'}`",
        ]
    )
    if publish_checklist:
        lines.extend(
            [
                "",
                "## Gate Readiness",
                "",
                f"- Internal review ready: `{publish_checklist.get('internal_review_ready')}`",
                f"- Publish ready: `{publish_checklist.get('publish_ready')}`",
            ]
        )
        disclosure = publish_checklist.get("quarterly_pipeline_disclosure") or {}
        if disclosure:
            reason = disclosure.get("display_reason")
            if reason == "forward_quarter_fallback":
                lines.append(
                    f"- Forward-quarter fallback: `{disclosure.get('quarterly_pipeline_title') or 'forward quarter'}`. "
                    f"{disclosure.get('quarterly_pipeline_footnote') or 'Fallback disclosure missing.'}"
                )
            elif reason == "empty_current_and_forward":
                lines.append(
                    f"- Empty current and forward quarter. "
                    f"{disclosure.get('quarterly_pipeline_footnote') or 'Empty-state note missing.'}"
                )
            else:
                lines.append("- Current-quarter pipeline remains in scope.")
    blockers = summary.get("publish_blockers") or []
    if blockers:
        lines.extend(["", "## Publish Blockers"])
        lines.extend(f"- {blocker}" for blocker in blockers if isinstance(blocker, str))
    notes = summary.get("notes") or []
    if notes:
        lines.extend(["", "## Caveats"])
        lines.extend(f"- {note}" for note in notes if isinstance(note, str))
    return "\n".join(lines) + "\n"


def build_latest_status_packet(*, manifest: dict[str, Any]) -> dict[str, Any]:
    publish_checklist = manifest.get("publish_checklist") or {}
    blocked_checks = [
        {
            "name": check.get("name"),
            "detail": check.get("detail"),
        }
        for check in (publish_checklist.get("checks") or [])
        if check.get("status") == "blocked"
    ]
    return {
        "artifact_type": "sales_director_monthly_latest_status",
        "snapshot_date": manifest.get("snapshot_date"),
        "quarter_focus": manifest.get("quarter_focus"),
        "run_dir": manifest.get("run_dir"),
        "deck_path": manifest.get("deck_path"),
        "run_summary_path": str(Path(manifest.get("run_dir") or "") / "RUN_SUMMARY.md")
        if manifest.get("run_dir")
        else None,
        "publish_checklist_path": manifest.get("publish_checklist_markdown_path"),
        "internal_review_packet_path": manifest.get("internal_review_packet_markdown_path"),
        "publish_ready": publish_checklist.get("publish_ready"),
        "blocked_item_count": publish_checklist.get("blocked_item_count"),
        "quarterly_pipeline_disclosure": publish_checklist.get("quarterly_pipeline_disclosure"),
        "validation_status": (manifest.get("validation_summary") or {}).get("status"),
        "powerpoint_review_status": (manifest.get("powerpoint_review_summary") or {}).get("status"),
        "blocked_checks": blocked_checks,
    }


def build_latest_status_markdown(*, packet: dict[str, Any]) -> str:
    lines = [
        "# Latest Sales Director Monthly Run",
        "",
        f"- Snapshot date: `{packet.get('snapshot_date')}`",
        f"- Quarter focus: `{packet.get('quarter_focus')}`",
        f"- Publish ready: `{packet.get('publish_ready')}`",
        f"- Blocked items: `{packet.get('blocked_item_count')}`",
        f"- Run dir: `{packet.get('run_dir')}`",
        f"- Deck: `{packet.get('deck_path')}`",
        f"- Run summary: `{packet.get('run_summary_path')}`",
        f"- Publish checklist: `{packet.get('publish_checklist_path')}`",
        f"- Internal review packet: `{packet.get('internal_review_packet_path')}`",
        f"- Validation status: `{packet.get('validation_status') or 'n/a'}`",
        f"- PowerPoint review status: `{packet.get('powerpoint_review_status') or 'n/a'}`",
        "",
        "## Quarter Disclosure",
        "",
    ]
    disclosure = packet.get("quarterly_pipeline_disclosure") or {}
    if disclosure:
        reason = disclosure.get("display_reason")
        if reason == "forward_quarter_fallback":
            lines.append(
                f"- Forward-quarter fallback: `{disclosure.get('quarterly_pipeline_title') or 'forward quarter'}`. "
                f"{disclosure.get('quarterly_pipeline_footnote') or 'Fallback disclosure missing.'}"
            )
        elif reason == "empty_current_and_forward":
            lines.append(
                f"- Empty current and forward quarter. "
                f"{disclosure.get('quarterly_pipeline_footnote') or 'Empty-state note missing.'}"
            )
        else:
            lines.append("- Current-quarter pipeline remains in scope.")
    else:
        lines.append("- Current-quarter pipeline remains in scope.")
    lines.extend(["", "## Blocked Checks", ""])
    if packet.get("blocked_checks"):
        for check in packet["blocked_checks"]:
            lines.append(f"- {check.get('name')}: {check.get('detail')}")
    else:
        lines.append("- None.")
    return "\n".join(lines) + "\n"


def write_latest_aliases(*, output_root: Path, packet: dict[str, Any], markdown: str) -> None:
    write_json(output_root / "latest.json", packet)
    write_text(output_root / "latest.md", markdown)


def build_commentary_request_pack(*, snapshot: dict[str, Any]) -> dict[str, Any]:
    slipped = snapshot.get("slipped_deals") or {}
    rows = slipped.get("top_repeat_push") or []
    items = []
    for idx, row in enumerate(rows[:8], start=1):
        items.append(
            {
                "priority": idx,
                "region": row.get("sales_region"),
                "account_name": row.get("account_name"),
                "opportunity_name": row.get("opportunity_name"),
                "owner_name": row.get("owner_name"),
                "forecast_category": row.get("forecast_category"),
                "stage_name": row.get("stage_name"),
                "weighted_open_arr": row.get("weighted_open_arr"),
                "push_count": row.get("push_count"),
                "days_in_stage": row.get("days_in_stage"),
                "questions": [
                    "What specifically caused this deal to slip from the expected close timing?",
                    "What must happen next to recover the deal into a credible plan?",
                    "Is the issue customer-side, internal, commercial, or scope-related?",
                ],
            }
        )
    return {
        "artifact_type": "sales_director_owner_commentary_request_pack",
        "snapshot_date": snapshot.get("snapshot_date"),
        "quarter_focus": snapshot.get("quarter_focus"),
        "item_count": len(items),
        "items": items,
    }


def build_commentary_request_markdown(*, pack: dict[str, Any]) -> str:
    lines = [
        "# Slipped-Deal Owner Commentary Request",
        "",
        f"- Snapshot date: `{pack.get('snapshot_date')}`",
        f"- Quarter focus: `{pack.get('quarter_focus')}`",
        f"- Items: `{pack.get('item_count')}`",
        "",
        "Use this pack to collect current-quarter owner commentary for the slipped-deals slide.",
    ]
    for item in pack.get("items") or []:
        lines.extend(
            [
                "",
                f"## Priority {item.get('priority')}",
                f"- Region: `{item.get('region')}`",
                f"- Account / Opportunity: `{item.get('account_name')} | {item.get('opportunity_name')}`",
                f"- Owner: `{item.get('owner_name')}`",
                f"- Stage: `{item.get('stage_name')}`",
                f"- Forecast category: `{item.get('forecast_category')}`",
                f"- Weighted open ARR: `{item.get('weighted_open_arr')}`",
                f"- Push count: `{item.get('push_count')}`",
                f"- Days in stage: `{item.get('days_in_stage')}`",
                "- Prompt questions:",
            ]
        )
        lines.extend(f"  - {question}" for question in item.get("questions") or [])
    return "\n".join(lines) + "\n"


def build_commentary_request_csv_rows(*, pack: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in pack.get("items") or []:
        rows.append(
            {
                "priority": item.get("priority") or "",
                "region": item.get("region") or "",
                "account_name": item.get("account_name") or "",
                "opportunity_name": item.get("opportunity_name") or "",
                "owner_name": item.get("owner_name") or "",
                "stage_name": item.get("stage_name") or "",
                "forecast_category": item.get("forecast_category") or "",
                "weighted_open_arr": item.get("weighted_open_arr") or "",
                "push_count": item.get("push_count") or "",
                "days_in_stage": item.get("days_in_stage") or "",
                "theme": "",
                "comment": "",
            }
        )
    return rows


def build_commentary_owner_rollup(*, pack: dict[str, Any]) -> dict[str, Any]:
    owners: dict[str, dict[str, Any]] = {}
    for item in pack.get("items") or []:
        owner_name = str(item.get("owner_name") or "Unassigned").strip() or "Unassigned"
        row = owners.setdefault(
            owner_name,
            {
                "owner_name": owner_name,
                "item_count": 0,
                "weighted_open_arr_total": 0.0,
                "regions": [],
                "opportunities": [],
            },
        )
        row["item_count"] += 1
        row["weighted_open_arr_total"] += float(item.get("weighted_open_arr") or 0)
        region = str(item.get("region") or "").strip()
        if region and region not in row["regions"]:
            row["regions"].append(region)
        opportunity_name = str(item.get("opportunity_name") or "").strip()
        if opportunity_name:
            row["opportunities"].append(opportunity_name)

    owner_rows = sorted(
        owners.values(),
        key=lambda row: (-float(row["weighted_open_arr_total"]), -int(row["item_count"]), row["owner_name"]),
    )
    return {
        "artifact_type": "sales_director_owner_commentary_owner_rollup",
        "snapshot_date": pack.get("snapshot_date"),
        "quarter_focus": pack.get("quarter_focus"),
        "owner_count": len(owner_rows),
        "owners": owner_rows,
    }


def slugify_owner_name(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "unassigned"


def build_commentary_owner_packet_markdown(
    *,
    pack: dict[str, Any],
    owner_name: str,
    items: list[dict[str, Any]],
) -> str:
    total_weighted_open_arr = sum(float(item.get("weighted_open_arr") or 0) for item in items)
    regions = sorted({str(item.get("region") or "").strip() for item in items if str(item.get("region") or "").strip()})
    lines = [
        f"# Commentary Packet: {owner_name}",
        "",
        f"- Snapshot date: `{pack.get('snapshot_date')}`",
        f"- Quarter focus: `{pack.get('quarter_focus')}`",
        f"- Items: `{len(items)}`",
        f"- Weighted open ARR total: `{total_weighted_open_arr}`",
        f"- Regions: `{', '.join(regions) or 'n/a'}`",
        "",
        "## Reply Format",
        "",
        "- `theme`: short label such as procurement delay, budget hold, scope reset, legal review, internal pricing, or customer decision timing",
        "- `comment`: one sentence on what caused the slip and what must happen next to recover it",
        "",
        "## Opportunities In Scope",
    ]
    for idx, item in enumerate(items, start=1):
        lines.extend(
            [
                "",
                f"### {idx}. {item.get('account_name')} | {item.get('opportunity_name')}",
                f"- Region: `{item.get('region')}`",
                f"- Stage: `{item.get('stage_name')}`",
                f"- Forecast category: `{item.get('forecast_category')}`",
                f"- Weighted open ARR: `{item.get('weighted_open_arr')}`",
                f"- Push count: `{item.get('push_count')}`",
                f"- Days in stage: `{item.get('days_in_stage')}`",
                "- Prompt questions:",
            ]
        )
        lines.extend(f"  - {question}" for question in item.get("questions") or [])
    return "\n".join(lines) + "\n"


def build_commentary_owner_packet_bundle(*, pack: dict[str, Any], packet_dir: Path) -> dict[str, Any]:
    owner_items: dict[str, list[dict[str, Any]]] = {}
    for item in pack.get("items") or []:
        owner_name = str(item.get("owner_name") or "Unassigned").strip() or "Unassigned"
        owner_items.setdefault(owner_name, []).append(item)

    packets: list[dict[str, Any]] = []
    packet_dir.mkdir(parents=True, exist_ok=True)
    for owner_name in sorted(owner_items):
        items = sorted(
            owner_items[owner_name],
            key=lambda row: (
                int(row.get("priority") or 9999),
                -float(row.get("weighted_open_arr") or 0),
                str(row.get("opportunity_name") or ""),
            ),
        )
        slug = slugify_owner_name(owner_name)
        packet_path = packet_dir / f"{slug}.md"
        write_text(
            packet_path,
            build_commentary_owner_packet_markdown(
                pack=pack,
                owner_name=owner_name,
                items=items,
            ),
        )
        packets.append(
            {
                "owner_name": owner_name,
                "slug": slug,
                "item_count": len(items),
                "weighted_open_arr_total": sum(float(item.get("weighted_open_arr") or 0) for item in items),
                "packet_markdown_path": str(packet_path),
                "regions": sorted(
                    {
                        str(item.get("region") or "").strip()
                        for item in items
                        if str(item.get("region") or "").strip()
                    }
                ),
                "example_opportunities": [
                    str(item.get("opportunity_name") or "").strip()
                    for item in items[:3]
                    if str(item.get("opportunity_name") or "").strip()
                ],
            }
        )

    return {
        "artifact_type": "sales_director_owner_commentary_packet_bundle",
        "snapshot_date": pack.get("snapshot_date"),
        "quarter_focus": pack.get("quarter_focus"),
        "owner_count": len(packets),
        "packets": packets,
        "packet_dir": str(packet_dir),
    }


def build_commentary_owner_packet_index_markdown(*, packet_bundle: dict[str, Any]) -> str:
    lines = [
        "# Owner Commentary Packet Index",
        "",
        f"- Snapshot date: `{packet_bundle.get('snapshot_date')}`",
        f"- Quarter focus: `{packet_bundle.get('quarter_focus')}`",
        f"- Owner packets: `{packet_bundle.get('owner_count')}`",
        "",
        "Use this index to send the right packet to each owner without rebuilding context from the shared CSV.",
    ]
    for packet in packet_bundle.get("packets") or []:
        lines.extend(
            [
                "",
                f"## {packet.get('owner_name')}",
                f"- Packet: `{packet.get('packet_markdown_path')}`",
                f"- Items: `{packet.get('item_count')}`",
                f"- Response status: `{packet.get('response_status') or 'pending'}`",
                f"- Weighted open ARR: `{packet.get('weighted_open_arr_total')}`",
                f"- Regions: `{', '.join(packet.get('regions') or []) or 'n/a'}`",
                f"- Example opportunities: `{', '.join(packet.get('example_opportunities') or []) or 'n/a'}`",
            ]
        )
    return "\n".join(lines) + "\n"


def build_commentary_owner_summary_markdown(*, rollup: dict[str, Any]) -> str:
    lines = [
        "# Slipped-Deal Commentary Owner Summary",
        "",
        f"- Snapshot date: `{rollup.get('snapshot_date')}`",
        f"- Quarter focus: `{rollup.get('quarter_focus')}`",
        f"- Owners in follow-up pack: `{rollup.get('owner_count')}`",
        "",
        "Use this as the quick send list for owner follow-up. The detailed opportunity rows remain in `owner_commentary_request.csv`.",
    ]
    for row in rollup.get("owners") or []:
        regions = ", ".join(row.get("regions") or []) or "n/a"
        opportunities = ", ".join((row.get("opportunities") or [])[:3]) or "n/a"
        lines.extend(
            [
                "",
                f"## {row.get('owner_name')}",
                f"- Items: `{row.get('item_count')}`",
                f"- Response status: `{row.get('response_status') or 'pending'}`",
                f"- Weighted open ARR: `{row.get('weighted_open_arr_total')}`",
                f"- Regions: `{regions}`",
                f"- Example opportunities: `{opportunities}`",
            ]
        )
    return "\n".join(lines) + "\n"


def build_commentary_owner_response_summary(*, slipped_overlay: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    owner_comments = (slipped_overlay or {}).get("owner_comments") if isinstance(slipped_overlay, dict) else []
    rows = owner_comments if isinstance(owner_comments, list) else []
    by_owner: dict[str, dict[str, Any]] = {}
    for row in rows:
        owner_name = str((row or {}).get("owner_name") or "").strip()
        if not owner_name:
            continue
        summary = by_owner.setdefault(owner_name, {"provided_comment_count": 0})
        summary["provided_comment_count"] += 1
    return by_owner


def build_commentary_owner_send_list_rows(
    *,
    rollup: dict[str, Any],
    packet_bundle: dict[str, Any] | None = None,
    slipped_overlay: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    packet_lookup = {
        str(packet.get("owner_name") or ""): packet for packet in (packet_bundle or {}).get("packets") or []
    }
    response_lookup = build_commentary_owner_response_summary(slipped_overlay=slipped_overlay)
    rows: list[dict[str, Any]] = []
    for row in rollup.get("owners") or []:
        owner_name = row.get("owner_name") or ""
        packet = packet_lookup.get(str(owner_name), {})
        response = response_lookup.get(str(owner_name), {})
        item_count = int(row.get("item_count") or 0)
        provided_comment_count = int(response.get("provided_comment_count") or 0)
        pending_comment_count = max(item_count - provided_comment_count, 0)
        response_status = (
            "complete"
            if item_count > 0 and pending_comment_count == 0 and provided_comment_count > 0
            else "partial"
            if provided_comment_count > 0
            else "pending"
        )
        packet["response_status"] = response_status
        row["response_status"] = response_status
        rows.append(
            {
                "owner_name": owner_name,
                "item_count": item_count,
                "weighted_open_arr_total": row.get("weighted_open_arr_total") or 0,
                "regions": ", ".join(row.get("regions") or []),
                "example_opportunities": ", ".join((row.get("opportunities") or [])[:3]),
                "response_status": response_status,
                "provided_comment_count": provided_comment_count,
                "pending_comment_count": pending_comment_count,
                "suggested_subject": (
                    f"Input needed: slipped-deal commentary for {owner_name}"
                    if owner_name
                    else "Input needed: slipped-deal commentary"
                ),
                "owner_packet_markdown_path": packet.get("packet_markdown_path") or "",
            }
        )
    return rows


def build_commentary_request_email(*, pack: dict[str, Any]) -> str:
    owner_rollup = build_commentary_owner_rollup(pack=pack)
    top_owners = owner_rollup.get("owners") or []
    owner_line = ", ".join(
        f"{row['owner_name']} ({row['item_count']})" for row in top_owners[:5] if row.get("owner_name")
    )
    lines = [
        "Subject: Input needed for slipped-deal commentary in this month’s Sales Director pack",
        "",
        "Team,",
        "",
        "We are finalizing the monthly Sales Director pipeline pack and need short root-cause commentary for the slipped deals below.",
        "Please reply in the attached CSV using the `theme` and `comment` fields, or send one short note per opportunity in the same structure.",
        "This collection pack is already scoped to the highest-value repeat pushes in the current-quarter queue.",
        "",
        "Response format:",
        "- `theme`: short label such as procurement delay, budget hold, scope reset, legal review, internal pricing, or customer decision timing",
        "- `comment`: one sentence on what caused the slip and what must happen next to recover it",
        "",
        f"Quarter focus: {pack.get('quarter_focus')}",
        f"Snapshot date: {pack.get('snapshot_date')}",
        f"Owners in this request pack: {owner_rollup.get('owner_count')}",
        "",
        "The attached `owner_commentary_request.csv` is already prefilled with the priority opportunities.",
    ]
    if owner_line:
        lines.extend(["", f"Primary owners in scope: {owner_line}"])
    return "\n".join(lines) + "\n"


def build_finance_churn_request_pack(*, snapshot: dict[str, Any]) -> dict[str, Any]:
    churn = snapshot.get("churn") or {}
    external_inputs = snapshot.get("external_inputs") or {}
    finance_overlay = external_inputs.get("finance_churn") or {}

    items = []
    for idx, row in enumerate((churn.get("top_churned_renewals") or [])[:6], start=1):
        items.append(
            {
                "priority": idx,
                "account_name": row.get("account_name"),
                "opportunity_name": row.get("opportunity_name"),
                "owner_name": row.get("owner_name"),
                "historical_quarter": row.get("quarter_label"),
                "historical_outcome": row.get("outcome"),
                "historical_stage": row.get("stage"),
                "historical_churn_acv": row.get("renewal_acv"),
                "historical_amount": row.get("amount"),
                "questions": [
                    "Should this account be carried in the current Finance forward-risk view?",
                    "If yes, what amount and short risk signal should appear on the slide?",
                    "If no, which live account should replace it in the Finance view this month?",
                ],
            }
        )

    owner_concentration = []
    for row in (churn.get("top_owners") or [])[:3]:
        owner_concentration.append(
            {
                "owner_name": row.get("owner_name"),
                "churned_acv": row.get("churned_acv"),
                "churned_deals": row.get("churned_deals"),
            }
        )

    return {
        "artifact_type": "sales_director_finance_churn_request_pack",
        "snapshot_date": snapshot.get("snapshot_date"),
        "quarter_focus": snapshot.get("quarter_focus"),
        "finance_feed_status": churn.get("finance_feed_status") or "unknown",
        "current_overlay_status": finance_overlay.get("status") or "pending",
        "overlay_contract": {
            "owner": finance_overlay.get("owner") or "",
            "source_name": finance_overlay.get("source_name") or "",
            "headline": finance_overlay.get("headline") or "",
            "summary_note": finance_overlay.get("summary_note") or "",
            "top_accounts_required_fields": ["account_name", "region", "signal", "amount"],
        },
        "historical_anchor_count": len(items),
        "owner_concentration": owner_concentration,
        "items": items,
    }


def build_finance_churn_request_markdown(*, pack: dict[str, Any]) -> str:
    lines = [
        "# Finance Churn Overlay Request",
        "",
        f"- Snapshot date: `{pack.get('snapshot_date')}`",
        f"- Quarter focus: `{pack.get('quarter_focus')}`",
        f"- Finance feed status: `{pack.get('finance_feed_status')}`",
        f"- Current overlay status: `{pack.get('current_overlay_status')}`",
        f"- Historical anchors: `{pack.get('historical_anchor_count')}`",
        "",
        "Use this pack to replace the placeholder churn slide with the current Finance forward-risk view.",
        "",
        "## Requested Overlay Fields",
        "",
    ]
    overlay_contract = pack.get("overlay_contract") or {}
    lines.extend(
        [
            f"- `owner`: `{overlay_contract.get('owner') or 'Fill with Finance contact or team name'}`",
            f"- `source_name`: `{overlay_contract.get('source_name') or 'Name the Finance churn source used for this month'}`",
            f"- `headline`: `{overlay_contract.get('headline') or 'Provide the one-line Finance churn readout for the slide'}`",
            (
                "- `summary_note`: "
                f"`{overlay_contract.get('summary_note') or 'State refresh date, scope, and caveats Finance wants carried'}`"
            ),
            "- `top_accounts`: provide 3-5 live accounts with `account_name`, `region`, `signal`, and `amount`.",
            "- Put the overlay-level fields in the first populated row of `finance_churn_request.csv`; the merge utility reads the first non-empty value for each.",
        ]
    )

    owner_concentration = pack.get("owner_concentration") or []
    if owner_concentration:
        lines.extend(["", "## Historical Concentration Context", ""])
        for row in owner_concentration:
            lines.append(
                f"- `{row.get('owner_name')}`: ACV `{row.get('churned_acv')}` across `{row.get('churned_deals')}` deals"
            )

    for item in pack.get("items") or []:
        lines.extend(
            [
                "",
                f"## Priority {item.get('priority')}",
                f"- Account / Opportunity: `{item.get('account_name')} | {item.get('opportunity_name')}`",
                f"- Historical owner: `{item.get('owner_name')}`",
                f"- Historical quarter: `{item.get('historical_quarter')}`",
                f"- Historical outcome: `{item.get('historical_outcome')}`",
                f"- Historical stage: `{item.get('historical_stage')}`",
                f"- Historical churn ACV: `{item.get('historical_churn_acv')}`",
                f"- Historical amount: `{item.get('historical_amount')}`",
                "- Prompt questions:",
            ]
        )
        lines.extend(f"  - {question}" for question in item.get("questions") or [])
    return "\n".join(lines) + "\n"


def build_finance_churn_request_csv_rows(*, pack: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in pack.get("items") or []:
        rows.append(
            {
                "priority": item.get("priority") or "",
                "account_name": item.get("account_name") or "",
                "opportunity_name": item.get("opportunity_name") or "",
                "owner_name": item.get("owner_name") or "",
                "historical_quarter": item.get("historical_quarter") or "",
                "historical_outcome": item.get("historical_outcome") or "",
                "historical_stage": item.get("historical_stage") or "",
                "historical_churn_acv": item.get("historical_churn_acv") or "",
                "historical_amount": item.get("historical_amount") or "",
                "overlay_owner": "",
                "overlay_source_name": "",
                "overlay_headline": "",
                "overlay_summary_note": "",
                "include_in_forward_risk": "",
                "region": "",
                "signal": "",
                "amount": "",
                "note": "",
            }
        )
    return rows


def build_finance_churn_request_email(*, pack: dict[str, Any]) -> str:
    owner_concentration = pack.get("owner_concentration") or []
    owner_line = ", ".join(
        str(row.get("owner_name") or "").strip()
        for row in owner_concentration
        if str(row.get("owner_name") or "").strip()
    )
    lines = [
        "Subject: Input needed: Finance churn overlay for the Sales Director monthly deck",
        "",
        "Hi Finance team,",
        "",
        "We are finalizing the monthly Sales Director deck and need the live Finance churn overlay for the current-quarter risk slide.",
        "",
        "Please update the attached `finance_churn_request.csv`, filling the overlay header fields on the first populated row and confirming the four deck-level fields below:",
        "",
        "- `owner`: Finance contact or team name",
        "- `source_name`: name of the Finance churn source used this month",
        "- `headline`: one-line Finance risk readout for the slide",
        "- `summary_note`: refresh date, scope, and caveats we should carry in the deck",
        "",
        f"Quarter focus: {pack.get('quarter_focus')}",
        f"Snapshot date: {pack.get('snapshot_date')}",
        f"Historical anchors attached: {pack.get('historical_anchor_count')}",
        "",
        "Each CSV row includes a historical churn anchor. For each one, confirm whether it should remain in the live forward-risk view. If not, replace it with the live account that should be shown on the slide.",
    ]
    if owner_line:
        lines.extend(["", f"Historical concentration context: {owner_line}"])
    return "\n".join(lines) + "\n"


def build_overlay_fill_template(*, commentary_pack: dict[str, Any]) -> dict[str, Any]:
    return {
        "finance_churn": {
            "status": "pending",
            "provenance": "pending",
            "owner": "",
            "source_name": "",
            "headline": "",
            "summary_note": "",
            "top_accounts": [],
        },
        "slipped_commentary": {
            "status": "pending",
            "provenance": "external",
            "summary_note": "",
            "root_cause_bullets": [],
            "coverage_status": "pending",
            "requested_item_count": len(commentary_pack.get("items") or []),
            "provided_comment_count": 0,
            "pending_comment_count": len(commentary_pack.get("items") or []),
            "requested_owner_count": len(
                {
                    str(item.get("owner_name") or "").strip()
                    for item in commentary_pack.get("items") or []
                    if str(item.get("owner_name") or "").strip()
                }
            ),
            "responded_owner_count": 0,
            "pending_owner_count": len(
                {
                    str(item.get("owner_name") or "").strip()
                    for item in commentary_pack.get("items") or []
                    if str(item.get("owner_name") or "").strip()
                }
            ),
            "pending_owner_names": sorted(
                {
                    str(item.get("owner_name") or "").strip()
                    for item in commentary_pack.get("items") or []
                    if str(item.get("owner_name") or "").strip()
                }
            ),
            "owner_comments": [
                {
                    "owner_name": item.get("owner_name") or "",
                    "region": item.get("region") or "",
                    "opportunity_name": item.get("opportunity_name") or "",
                    "theme": "",
                    "comment": "",
                }
                for item in commentary_pack.get("items") or []
            ],
        },
    }


def build_approval_rule_contract(*, snapshot: dict[str, Any]) -> dict[str, Any]:
    approval = snapshot.get("commercial_approval") or {}
    rule = approval.get("rule_contract") or {}
    quarter_window = snapshot.get("quarter_window") or {}
    return {
        "artifact_type": "sales_director_approval_rule_contract",
        "snapshot_date": snapshot.get("snapshot_date"),
        "quarter_focus": snapshot.get("quarter_focus"),
        "rule_status": rule.get("status") or "unknown",
        "rule_label": rule.get("label") or "",
        "dataset": rule.get("dataset") or "",
        "quarter_window": quarter_window,
        "candidate_logic": rule.get("candidate_logic") or [],
        "control_logic": rule.get("control_logic") or [],
        "current_snapshot": {
            "approved_count": (approval.get("summary") or {}).get("approved_count"),
            "pending_count": (approval.get("summary") or {}).get("pending_count"),
            "stale_count": (approval.get("summary") or {}).get("stale_count"),
            "candidate_count": approval.get("candidate_count"),
            "control_exception_count": len(approval.get("control_exceptions") or []),
        },
    }


def build_approval_rule_markdown(*, contract: dict[str, Any]) -> str:
    lines = [
        "# Commercial Approval Rule Contract",
        "",
        f"- Snapshot date: `{contract.get('snapshot_date')}`",
        f"- Quarter focus: `{contract.get('quarter_focus')}`",
        f"- Rule status: `{contract.get('rule_status')}`",
        f"- Rule label: `{contract.get('rule_label')}`",
        f"- Dataset: `{contract.get('dataset')}`",
    ]
    quarter_window = contract.get("quarter_window") or {}
    if quarter_window:
        lines.extend(
            [
                f"- Quarter window: `{quarter_window.get('start_date')}` to `{quarter_window.get('end_date')}`",
                f"- Days remaining in quarter at snapshot: `{quarter_window.get('days_remaining')}`",
            ]
        )
    candidate_logic = contract.get("candidate_logic") or []
    if candidate_logic:
        lines.extend(["", "## Candidate Logic"])
        lines.extend(f"- {item}" for item in candidate_logic if isinstance(item, str))
    control_logic = contract.get("control_logic") or []
    if control_logic:
        lines.extend(["", "## Control Logic"])
        lines.extend(f"- {item}" for item in control_logic if isinstance(item, str))
    current_snapshot = contract.get("current_snapshot") or {}
    lines.extend(
        [
            "",
            "## Current Snapshot",
            "",
            f"- Approved count: `{current_snapshot.get('approved_count')}`",
            f"- Pending count: `{current_snapshot.get('pending_count')}`",
            f"- Stale count: `{current_snapshot.get('stale_count')}`",
            f"- Candidate count: `{current_snapshot.get('candidate_count')}`",
            f"- Control exception count: `{current_snapshot.get('control_exception_count')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def build_publish_checklist(
    *,
    snapshot: dict[str, Any],
    deck_summary: dict[str, Any],
    thumbnail_path: str | None,
    validation_summary: dict[str, Any] | None,
    powerpoint_review_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    external_inputs = snapshot.get("external_inputs") or {}
    finance = external_inputs.get("finance_churn") or {}
    slipped = external_inputs.get("slipped_commentary") or {}
    approval = snapshot.get("commercial_approval") or {}
    renewals = snapshot.get("renewals") or {}
    churn = snapshot.get("churn") or {}

    finance_publishable = finance.get("status") == "provided" and finance.get("provenance") not in {"example", "pending", ""}
    slipped_coverage_status = str(slipped.get("coverage_status") or "").lower()
    slipped_requested_count = as_int(slipped.get("requested_item_count"))
    slipped_provided_count = as_int(slipped.get("provided_comment_count"))
    slipped_pending_count = as_int(slipped.get("pending_comment_count"))
    slipped_publishable = (
        slipped.get("status") == "provided"
        and slipped.get("provenance") not in {"example", "pending", ""}
        and slipped_coverage_status != "partial"
    )
    approval_rule_aligned = (approval.get("rule_contract") or {}).get("status") == "aligned_with_report_target"
    renewals_scoped = (renewals.get("selection_scope") or {}).get("status") == "aligned_with_report_target"
    renewals_methodology_aligned = (renewals.get("metric_contract") or {}).get("status") == "aligned_with_simcorp_methodology"
    churn_methodology_aligned = (churn.get("metric_contract") or {}).get("status") == "aligned_with_simcorp_methodology"
    quarterly_pipeline_disclosure = extract_quarterly_pipeline_disclosure(snapshot)
    disclosure_reason = (quarterly_pipeline_disclosure or {}).get("display_reason")
    disclosure_title = (quarterly_pipeline_disclosure or {}).get("quarterly_pipeline_title") or "forward quarter"
    disclosure_footnote = (quarterly_pipeline_disclosure or {}).get("quarterly_pipeline_footnote")
    if disclosure_reason == "forward_quarter_fallback":
        quarterly_pipeline_status = "pass" if disclosure_footnote else "blocked"
        quarterly_pipeline_detail = (
            disclosure_footnote
            or f"Forward-quarter fallback to {disclosure_title} is missing an explicit footnote."
        )
    elif disclosure_reason == "empty_current_and_forward":
        quarterly_pipeline_status = "warn" if disclosure_footnote else "blocked"
        quarterly_pipeline_detail = (
            disclosure_footnote
            or "No current or forward-quarter pipeline is available and the empty-state note is missing."
        )
    else:
        quarterly_pipeline_status = "pass"
        quarterly_pipeline_detail = "Current-quarter pipeline remains in scope."

    checks = [
        {
            "name": "Live snapshot generated",
            "status": "pass" if snapshot.get("snapshot_date") else "blocked",
            "detail": f"Snapshot date {snapshot.get('snapshot_date')} for {snapshot.get('quarter_focus')}.",
        },
        {
            "name": "Deck generated",
            "status": "pass" if deck_summary.get("output") else "blocked",
            "detail": f"{deck_summary.get('slide_count')} slide pack generated.",
        },
        {
            "name": "Approval rule aligned to target",
            "status": "pass" if approval_rule_aligned else "blocked",
            "detail": (approval.get("rule_contract") or {}).get("label") or "Approval rule contract missing.",
        },
        {
            "name": "Renewals scoped to quarter",
            "status": "pass" if renewals_scoped else "blocked",
            "detail": (renewals.get("selection_scope") or {}).get("label") or "Renewal scope contract missing.",
        },
        {
            "name": "Renewal and churn methodology aligned",
            "status": "pass" if renewals_methodology_aligned and churn_methodology_aligned else "blocked",
            "detail": (renewals.get("metric_contract") or {}).get("label")
            or (churn.get("metric_contract") or {}).get("label")
            or "Renewal/churn metric contract missing.",
        },
        {
            "name": "Quarter fallback disclosed",
            "status": quarterly_pipeline_status,
            "detail": quarterly_pipeline_detail,
        },
        {
            "name": "Finance churn overlay publishable",
            "status": "pass" if finance_publishable else "blocked",
            "detail": f"Status={finance.get('status')} provenance={finance.get('provenance')}",
        },
        {
            "name": "Slipped commentary publishable",
            "status": "pass" if slipped_publishable else "blocked",
            "detail": (
                f"Status={slipped.get('status')} provenance={slipped.get('provenance')} "
                f"coverage={slipped_coverage_status or 'n/a'} "
                f"provided={slipped_provided_count} requested={slipped_requested_count} pending={slipped_pending_count}"
            ),
        },
        {
            "name": "Quick Look thumbnail generated",
            "status": "pass" if thumbnail_path else "warn",
            "detail": thumbnail_path or "Thumbnail generation skipped or unavailable.",
        },
        {
            "name": "Rendered validation bundle generated",
            "status": "pass" if (validation_summary or {}).get("status") == "ok" else "warn",
            "detail": (validation_summary or {}).get("detail")
            or "Validation bundle not generated.",
        },
        {
            "name": "PowerPoint-first review bundle generated",
            "status": "pass" if (powerpoint_review_summary or {}).get("status") == "ok" else "warn",
            "detail": (powerpoint_review_summary or {}).get("detail")
            or "PowerPoint-first review bundle not generated.",
        },
    ]
    blocked = [check for check in checks if check["status"] == "blocked"]
    return {
        "artifact_type": "sales_director_monthly_publish_checklist",
        "snapshot_date": snapshot.get("snapshot_date"),
        "quarter_focus": snapshot.get("quarter_focus"),
        "internal_review_ready": True,
        "publish_ready": len(blocked) == 0,
        "blocked_item_count": len(blocked),
        "quarterly_pipeline_disclosure": quarterly_pipeline_disclosure,
        "checks": checks,
    }


def build_publish_checklist_markdown(*, checklist: dict[str, Any]) -> str:
    lines = [
        "# Publish Checklist",
        "",
        f"- Snapshot date: `{checklist.get('snapshot_date')}`",
        f"- Quarter focus: `{checklist.get('quarter_focus')}`",
        f"- Internal review ready: `{checklist.get('internal_review_ready')}`",
        f"- Publish ready: `{checklist.get('publish_ready')}`",
        f"- Blocked items: `{checklist.get('blocked_item_count')}`",
        "",
    ]
    disclosure = checklist.get("quarterly_pipeline_disclosure") or {}
    lines.extend(["## Quarter Disclosure", ""])
    if disclosure:
        reason = disclosure.get("display_reason")
        if reason == "forward_quarter_fallback":
            lines.append(
                f"- Forward-quarter fallback: `{disclosure.get('quarterly_pipeline_title') or 'forward quarter'}`. "
                f"{disclosure.get('quarterly_pipeline_footnote') or 'Fallback disclosure missing.'}"
            )
        elif reason == "empty_current_and_forward":
            lines.append(
                f"- Empty current and forward quarter. "
                f"{disclosure.get('quarterly_pipeline_footnote') or 'Empty-state note missing.'}"
            )
        else:
            lines.append("- Current-quarter pipeline remains in scope.")
    else:
        lines.append("- Current-quarter pipeline remains in scope.")
    lines.extend(["", "## Checks", ""])
    for check in checklist.get("checks") or []:
        lines.append(f"- `{check.get('status')}` {check.get('name')}: {check.get('detail')}")
    return "\n".join(lines) + "\n"


def build_internal_review_packet(
    *,
    manifest: dict[str, Any],
    snapshot: dict[str, Any],
    deck_summary: dict[str, Any],
    publish_checklist: dict[str, Any],
    validation_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = deck_summary or {}
    blockers = list(summary.get("publish_blockers") or [])
    slipped = snapshot.get("slipped_deals") or {}
    top_repeat = (slipped.get("top_repeat_push") or [None])[0]
    quarterly_pipeline_disclosure = publish_checklist.get("quarterly_pipeline_disclosure") or extract_quarterly_pipeline_disclosure(snapshot)
    return {
        "artifact_type": "sales_director_internal_review_packet",
        "snapshot_date": snapshot.get("snapshot_date"),
        "quarter_focus": snapshot.get("quarter_focus"),
        "deck_path": manifest.get("deck_path"),
        "thumbnail_path": manifest.get("thumbnail_path"),
        "validation_montage_path": (validation_summary or {}).get("montage_path"),
        "powerpoint_review_pdf_path": ((manifest.get("powerpoint_review_summary") or {}).get("pdf_path")),
        "powerpoint_review_montage_path": ((manifest.get("powerpoint_review_summary") or {}).get("montage_path")),
        "publish_checklist_path": manifest.get("publish_checklist_markdown_path"),
        "run_summary_path": str(Path(manifest["run_dir"]) / "RUN_SUMMARY.md"),
        "owner_commentary_request_path": manifest.get("owner_commentary_markdown_path"),
        "owner_commentary_send_list_path": manifest.get("owner_commentary_owner_send_list_path"),
        "owner_commentary_packet_index_path": manifest.get("owner_commentary_owner_packet_index_markdown_path"),
        "finance_churn_request_path": manifest.get("finance_churn_request_markdown_path"),
        "finance_churn_request_csv_path": manifest.get("finance_churn_request_csv_path"),
        "finance_churn_request_email_path": manifest.get("finance_churn_request_email_path"),
        "approval_rule_contract_path": manifest.get("approval_rule_markdown_path"),
        "internal_review_ready": publish_checklist.get("internal_review_ready"),
        "publish_ready": publish_checklist.get("publish_ready"),
        "blocked_item_count": publish_checklist.get("blocked_item_count"),
        "quarterly_pipeline_disclosure": quarterly_pipeline_disclosure,
        "primary_readout": {
            "biggest_gap_region": summary.get("biggest_gap_region"),
            "biggest_gap_arr": summary.get("biggest_gap_arr"),
            "weakest_confidence_region": summary.get("weakest_confidence_region"),
            "weakest_confidence_pct": summary.get("weakest_confidence_pct"),
            "approval_candidate_count": summary.get("approval_candidate_count"),
            "quarter_renewal_pipeline_acv": summary.get("total_open_renewal_pipeline_acv"),
            "biggest_slipped_region": summary.get("biggest_slipped_region"),
            "biggest_slipped_arr": summary.get("biggest_slipped_arr"),
            "top_repeat_push_account": top_repeat.get("account_name") if isinstance(top_repeat, dict) else None,
        },
        "publish_blockers": blockers,
        "reviewer_focus": [
            "Confirm the regional reads land the right executive answer for each geography.",
            "Confirm the renewal action queue reflects the real month-close priorities.",
            "Confirm the slipped-deal commentary request pack is the right owner send list once outreach starts.",
        ],
    }


def build_internal_review_packet_markdown(*, packet: dict[str, Any]) -> str:
    readout = packet.get("primary_readout") or {}
    lines = [
        "# Internal Review Packet",
        "",
        f"- Snapshot date: `{packet.get('snapshot_date')}`",
        f"- Quarter focus: `{packet.get('quarter_focus')}`",
        f"- Internal review ready: `{packet.get('internal_review_ready')}`",
        f"- Publish ready: `{packet.get('publish_ready')}`",
        f"- Blocked items: `{packet.get('blocked_item_count')}`",
        "",
        "## Review Assets",
        "",
        f"- Deck: `{packet.get('deck_path')}`",
        f"- Run summary: `{packet.get('run_summary_path')}`",
        f"- Publish checklist: `{packet.get('publish_checklist_path')}`",
        f"- Thumbnail: `{packet.get('thumbnail_path')}`",
        f"- Validation montage: `{packet.get('validation_montage_path')}`",
        f"- PowerPoint review PDF: `{packet.get('powerpoint_review_pdf_path')}`",
        f"- PowerPoint review montage: `{packet.get('powerpoint_review_montage_path')}`",
        f"- Approval rule contract: `{packet.get('approval_rule_contract_path')}`",
        f"- Commentary request pack: `{packet.get('owner_commentary_request_path')}`",
        f"- Commentary owner send list: `{packet.get('owner_commentary_send_list_path')}`",
        f"- Commentary owner packet index: `{packet.get('owner_commentary_packet_index_path')}`",
        f"- Finance churn request pack: `{packet.get('finance_churn_request_path')}`",
        f"- Finance churn request CSV: `{packet.get('finance_churn_request_csv_path')}`",
        f"- Finance churn request email: `{packet.get('finance_churn_request_email_path')}`",
        "",
    ]
    disclosure = packet.get("quarterly_pipeline_disclosure") or {}
    lines.extend(["## Quarter Disclosure", ""])
    if disclosure:
        reason = disclosure.get("display_reason")
        if reason == "forward_quarter_fallback":
            lines.append(
                f"- Forward-quarter fallback: `{disclosure.get('quarterly_pipeline_title') or 'forward quarter'}`. "
                f"{disclosure.get('quarterly_pipeline_footnote') or 'Fallback disclosure missing.'}"
            )
        elif reason == "empty_current_and_forward":
            lines.append(
                f"- Empty current and forward quarter. "
                f"{disclosure.get('quarterly_pipeline_footnote') or 'Empty-state note missing.'}"
            )
        else:
            lines.append("- Current-quarter pipeline remains in scope.")
    else:
        lines.append("- Current-quarter pipeline remains in scope.")

    lines.extend([
        "",
        "## Executive Readout",
        "",
        f"- Biggest coverage gap: `{readout.get('biggest_gap_region')}` at `{readout.get('biggest_gap_arr')}`",
        f"- Weakest confidence region: `{readout.get('weakest_confidence_region')}` at `{readout.get('weakest_confidence_pct')}`",
        f"- Approval candidate count: `{readout.get('approval_candidate_count')}`",
        f"- Quarter renewal pipeline ACV: `{readout.get('quarter_renewal_pipeline_acv')}`",
        f"- Biggest slipped region: `{readout.get('biggest_slipped_region')}` at `{readout.get('biggest_slipped_arr')}`",
    ])
    if readout.get("top_repeat_push_account"):
        lines.append(f"- Highest-value repeat push account: `{readout.get('top_repeat_push_account')}`")

    blockers = packet.get("publish_blockers") or []
    if blockers:
        lines.extend(["", "## Current Blockers", ""])
        lines.extend(f"- {blocker}" for blocker in blockers if isinstance(blocker, str))

    lines.extend(["", "## Reviewer Focus", ""])
    lines.extend(f"- {item}" for item in packet.get("reviewer_focus") or [])
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    run_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (DEFAULT_OUTPUT_ROOT / f"{timestamp}_{args.snapshot_date}").resolve()
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    md1_preset_context = build_md1_preset_context(
        preset_config_path=args.md1_preset_config,
        preset_name=args.md1_preset_name,
    )
    md1_preset_context_path = None
    if md1_preset_context is not None:
        md1_preset_context_path = run_dir / "md1_preset_context.json"
        write_json(md1_preset_context_path, md1_preset_context)

    snapshot_path = run_dir / "report1_snapshot.json"
    deck_path = run_dir / f"sales_director_monthly_pipeline_insights_{args.snapshot_date}.pptx"
    deck_summary_path = run_dir / f"sales_director_monthly_pipeline_insights_{args.snapshot_date}.summary.json"
    overlay_prep = prepare_overlay_input(
        run_dir=run_dir,
        overlay_json=args.overlay_json,
        commentary_csv=args.commentary_csv,
        finance_csv=args.finance_csv,
        base_overlay=args.base_overlay,
        commentary_summary_note=args.commentary_summary_note,
        commentary_provenance=args.commentary_provenance,
        finance_provenance=args.finance_provenance,
    )

    snapshot_command = [
        sys.executable,
        str(SNAPSHOT_SCRIPT),
        "--snapshot-date",
        args.snapshot_date,
        "--output",
        str(snapshot_path),
        "--json",
    ]
    if overlay_prep["overlay_path"]:
        snapshot_command.extend(["--overlay-json", overlay_prep["overlay_path"]])
    snapshot_proc = run_command(command=snapshot_command, cwd=WORKSPACE)
    snapshot_payload = require_ok(snapshot_proc, label="snapshot refresh") or {}

    build_command = [
        "node",
        str(BUILD_SCRIPT),
        "--snapshot",
        str(snapshot_path),
        "--output",
        str(deck_path),
        "--summary-json",
        str(deck_summary_path),
        "--snapshot-date",
        args.snapshot_date,
        "--deck-title",
        args.deck_title,
        "--deck-subtitle",
        args.deck_subtitle,
    ]
    build_proc = run_command(command=build_command, cwd=WORKSPACE)
    build_payload = require_ok(build_proc, label="deck build") or {}

    snapshot_full = json.loads(snapshot_path.read_text(encoding="utf-8"))
    deck_summary = json.loads(deck_summary_path.read_text(encoding="utf-8"))
    commentary_pack = build_commentary_request_pack(snapshot=snapshot_full)
    finance_churn_pack = build_finance_churn_request_pack(snapshot=snapshot_full)
    commentary_owner_rollup = build_commentary_owner_rollup(pack=commentary_pack)
    approval_rule_contract = build_approval_rule_contract(snapshot=snapshot_full)
    commentary_pack_path = run_dir / "owner_commentary_request.json"
    commentary_markdown_path = run_dir / "owner_commentary_request.md"
    commentary_csv_path = run_dir / "owner_commentary_request.csv"
    commentary_email_path = run_dir / "owner_commentary_request_email.md"
    finance_churn_pack_path = run_dir / "finance_churn_request.json"
    finance_churn_markdown_path = run_dir / "finance_churn_request.md"
    finance_churn_csv_path = run_dir / "finance_churn_request.csv"
    finance_churn_email_path = run_dir / "finance_churn_request_email.md"
    commentary_owner_rollup_path = run_dir / "owner_commentary_owner_summary.json"
    commentary_owner_rollup_markdown_path = run_dir / "owner_commentary_owner_summary.md"
    commentary_owner_send_list_path = run_dir / "owner_commentary_owner_send_list.csv"
    commentary_owner_packet_dir = run_dir / "owner_commentary_owner_packets"
    commentary_owner_packet_bundle_path = run_dir / "owner_commentary_owner_packets.json"
    commentary_owner_packet_index_markdown_path = run_dir / "owner_commentary_owner_packets.md"
    approval_rule_contract_path = run_dir / "approval_rule_contract.json"
    approval_rule_markdown_path = run_dir / "approval_rule_contract.md"
    overlay_fill_template_path = run_dir / "report1_overlay.fill.json"
    commentary_owner_packet_bundle = build_commentary_owner_packet_bundle(
        pack=commentary_pack,
        packet_dir=commentary_owner_packet_dir,
    )
    slipped_overlay = (snapshot_full.get("external_inputs") or {}).get("slipped_commentary") or {}
    commentary_owner_send_list_rows = build_commentary_owner_send_list_rows(
        rollup=commentary_owner_rollup,
        packet_bundle=commentary_owner_packet_bundle,
        slipped_overlay=slipped_overlay if isinstance(slipped_overlay, dict) else {},
    )
    write_json(commentary_pack_path, commentary_pack)
    write_json(finance_churn_pack_path, finance_churn_pack)
    write_json(commentary_owner_rollup_path, commentary_owner_rollup)
    write_json(commentary_owner_packet_bundle_path, commentary_owner_packet_bundle)
    write_text(commentary_markdown_path, build_commentary_request_markdown(pack=commentary_pack))
    write_text(
        finance_churn_markdown_path,
        build_finance_churn_request_markdown(pack=finance_churn_pack),
    )
    write_text(
        commentary_owner_rollup_markdown_path,
        build_commentary_owner_summary_markdown(rollup=commentary_owner_rollup),
    )
    write_text(
        commentary_owner_packet_index_markdown_path,
        build_commentary_owner_packet_index_markdown(packet_bundle=commentary_owner_packet_bundle),
    )
    write_csv(
        commentary_owner_send_list_path,
        commentary_owner_send_list_rows,
        [
            "owner_name",
            "item_count",
            "weighted_open_arr_total",
            "regions",
            "example_opportunities",
            "response_status",
            "provided_comment_count",
            "pending_comment_count",
            "suggested_subject",
            "owner_packet_markdown_path",
        ],
    )
    write_csv(
        finance_churn_csv_path,
        build_finance_churn_request_csv_rows(pack=finance_churn_pack),
        [
            "priority",
            "account_name",
            "opportunity_name",
            "owner_name",
            "historical_quarter",
            "historical_outcome",
            "historical_stage",
            "historical_churn_acv",
            "historical_amount",
            "overlay_owner",
            "overlay_source_name",
            "overlay_headline",
            "overlay_summary_note",
            "include_in_forward_risk",
            "region",
            "signal",
            "amount",
            "note",
        ],
    )
    write_csv(
        commentary_csv_path,
        build_commentary_request_csv_rows(pack=commentary_pack),
        [
            "priority",
            "region",
            "account_name",
            "opportunity_name",
            "owner_name",
            "stage_name",
            "forecast_category",
            "weighted_open_arr",
            "push_count",
            "days_in_stage",
            "theme",
            "comment",
        ],
    )
    write_text(commentary_email_path, build_commentary_request_email(pack=commentary_pack))
    write_text(finance_churn_email_path, build_finance_churn_request_email(pack=finance_churn_pack))
    write_json(approval_rule_contract_path, approval_rule_contract)
    write_text(
        approval_rule_markdown_path,
        build_approval_rule_markdown(contract=approval_rule_contract),
    )
    write_json(
        overlay_fill_template_path,
        build_overlay_fill_template(commentary_pack=commentary_pack),
    )
    thumbnail_path = None if args.skip_thumbnail else maybe_make_thumbnail(deck_path=deck_path, run_dir=run_dir)
    validation_summary = maybe_validate_deck(
        deck_path=deck_path,
        run_dir=run_dir,
        skip_validation=args.skip_validation,
    )
    powerpoint_review_summary = maybe_generate_powerpoint_review(
        deck_path=deck_path,
        run_dir=run_dir,
    )
    publish_checklist = build_publish_checklist(
        snapshot=snapshot_full,
        deck_summary=deck_summary,
        thumbnail_path=thumbnail_path,
        validation_summary=validation_summary,
        powerpoint_review_summary=powerpoint_review_summary,
    )
    publish_checklist_path = run_dir / "publish_checklist.json"
    publish_checklist_markdown_path = run_dir / "publish_checklist.md"
    write_json(publish_checklist_path, publish_checklist)
    write_text(
        publish_checklist_markdown_path,
        build_publish_checklist_markdown(checklist=publish_checklist),
    )

    manifest = {
        "artifact_type": "sales_director_monthly_report_run",
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "snapshot_date": args.snapshot_date,
        "quarter_focus": snapshot_payload.get("quarter_focus"),
        "run_dir": str(run_dir),
        "md1_preset_context_path": str(md1_preset_context_path) if md1_preset_context_path else None,
        "md1_preset_context": md1_preset_context,
        "snapshot_path": str(snapshot_path),
        "deck_path": str(deck_path),
        "deck_summary_path": str(deck_summary_path),
        "overlay_path": snapshot_payload.get("overlay_path"),
        "requested_overlay_path": overlay_prep["requested_overlay_path"],
        "base_overlay_path": overlay_prep["base_overlay_path"],
        "commentary_csv_input_path": overlay_prep["commentary_csv_path"],
        "finance_csv_input_path": overlay_prep["finance_csv_path"],
        "merged_overlay_path": overlay_prep["merged_overlay_path"],
        "merged_owner_comment_count": overlay_prep["owner_comment_count"],
        "merged_finance_top_account_count": overlay_prep["finance_top_account_count"],
        "approval_rule_contract_path": str(approval_rule_contract_path),
        "approval_rule_markdown_path": str(approval_rule_markdown_path),
        "finance_churn_request_path": str(finance_churn_pack_path),
        "finance_churn_request_markdown_path": str(finance_churn_markdown_path),
        "finance_churn_request_csv_path": str(finance_churn_csv_path),
        "finance_churn_request_email_path": str(finance_churn_email_path),
        "owner_commentary_request_path": str(commentary_pack_path),
        "owner_commentary_markdown_path": str(commentary_markdown_path),
        "owner_commentary_csv_path": str(commentary_csv_path),
        "owner_commentary_email_path": str(commentary_email_path),
        "owner_commentary_owner_summary_path": str(commentary_owner_rollup_path),
        "owner_commentary_owner_summary_markdown_path": str(commentary_owner_rollup_markdown_path),
        "owner_commentary_owner_send_list_path": str(commentary_owner_send_list_path),
        "owner_commentary_owner_packet_dir": str(commentary_owner_packet_dir),
        "owner_commentary_owner_packet_bundle_path": str(commentary_owner_packet_bundle_path),
        "owner_commentary_owner_packet_index_markdown_path": str(
            commentary_owner_packet_index_markdown_path
        ),
        "overlay_fill_template_path": str(overlay_fill_template_path),
        "publish_checklist_path": str(publish_checklist_path),
        "publish_checklist_markdown_path": str(publish_checklist_markdown_path),
        "thumbnail_path": thumbnail_path,
        "validation_summary": validation_summary,
        "powerpoint_review_summary": powerpoint_review_summary,
        "snapshot_command": shlex.join(snapshot_command),
        "build_command": shlex.join(build_command),
        "snapshot_summary": snapshot_payload,
        "build_summary": build_payload,
        "deck_summary": deck_summary,
        "publish_checklist": publish_checklist,
    }
    internal_review_packet = build_internal_review_packet(
        manifest=manifest,
        snapshot=snapshot_full,
        deck_summary=deck_summary,
        publish_checklist=publish_checklist,
        validation_summary=validation_summary,
    )
    internal_review_packet_path = run_dir / "internal_review_packet.json"
    internal_review_packet_markdown_path = run_dir / "INTERNAL_REVIEW_PACKET.md"
    write_json(internal_review_packet_path, internal_review_packet)
    write_text(
        internal_review_packet_markdown_path,
        build_internal_review_packet_markdown(packet=internal_review_packet),
    )
    manifest["internal_review_packet_path"] = str(internal_review_packet_path)
    manifest["internal_review_packet_markdown_path"] = str(internal_review_packet_markdown_path)
    write_json(run_dir / "manifest.json", manifest)
    write_text(run_dir / "RUN_SUMMARY.md", build_run_summary(manifest=manifest))
    latest_status = build_latest_status_packet(manifest=manifest)
    write_latest_aliases(
        output_root=run_dir.parent,
        packet=latest_status,
        markdown=build_latest_status_markdown(packet=latest_status),
    )

    if args.json:
        print(json.dumps(manifest, indent=2))
    else:
        print(f"Wrote {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
