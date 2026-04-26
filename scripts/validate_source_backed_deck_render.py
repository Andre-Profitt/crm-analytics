#!/usr/bin/env python3
"""Render and validate source-backed PPTX decks through a headless PDF/PNG lane."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

from PIL import Image, ImageChops, ImageDraw, ImageStat
from pptx import Presentation


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "source_backed_deck_renders"
SCHEMA_VERSION = "monthly_platform.source_backed_deck_render_audit.v1"
DEFAULT_TIMEOUT_SECONDS = 90
MIN_PDF_BYTES = 20_000
MIN_PNG_BYTES = 10_000
MIN_IMAGE_STDDEV = 1.0


def validate_deck_render(
    *,
    deck_path: Path,
    output_path: Path | None = None,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    expected_slide_count: int | None = None,
    source_run_id: str | None = None,
    snapshot_date: str | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    deck_path = Path(deck_path)
    findings: list[dict[str, Any]] = []
    deck_slide_count = _deck_slide_count(deck_path, findings)
    expected = expected_slide_count or deck_slide_count
    resolved_snapshot_date = snapshot_date or "unknown-snapshot"
    resolved_run_id = source_run_id or "unknown-run"
    render_dir = _output_dir(
        output_path=output_path,
        output_root=output_root,
        snapshot_date=resolved_snapshot_date,
        source_run_id=resolved_run_id,
    )
    render_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = render_dir / f"{deck_path.stem}.pdf"
    slides_dir = render_dir / "slides"
    montage_path = render_dir / "montage.png"
    slides_dir.mkdir(parents=True, exist_ok=True)

    if not deck_path.exists():
        findings.append(_finding("high", "deck_missing", f"Missing deck: {deck_path}"))
        return _write_result(
            payload=_result_payload(
                status_inputs=RenderStatusInputs(
                    deck_path=deck_path,
                    pdf_path=pdf_path,
                    slides_dir=slides_dir,
                    montage_path=montage_path,
                    output_path=output_path,
                    snapshot_date=resolved_snapshot_date,
                    source_run_id=resolved_run_id,
                    deck_slide_count=deck_slide_count,
                    expected_slide_count=expected,
                    rendered_slide_paths=[],
                    findings=findings,
                )
            ),
            output_path=output_path,
            render_dir=render_dir,
        )

    _clean_render_targets(pdf_path=pdf_path, slides_dir=slides_dir, montage_path=montage_path)
    _render_pdf(
        deck_path=deck_path,
        pdf_path=pdf_path,
        timeout_seconds=timeout_seconds,
        findings=findings,
    )
    rendered_slide_paths: list[Path] = []
    if pdf_path.exists():
        _render_pngs(
            pdf_path=pdf_path,
            slides_dir=slides_dir,
            timeout_seconds=timeout_seconds,
            findings=findings,
        )
        rendered_slide_paths = sorted(slides_dir.glob("slide-*.png"), key=_slide_number)
        _validate_render_outputs(
            pdf_path=pdf_path,
            slide_paths=rendered_slide_paths,
            expected_slide_count=expected,
            findings=findings,
        )
        if rendered_slide_paths:
            _build_montage(rendered_slide_paths, montage_path)
            if not montage_path.exists() or montage_path.stat().st_size <= MIN_PNG_BYTES:
                findings.append(
                    _finding(
                        "high",
                        "montage_missing_or_too_small",
                        f"{montage_path}: {montage_path.stat().st_size if montage_path.exists() else 0} bytes",
                    )
                )

    return _write_result(
        payload=_result_payload(
            status_inputs=RenderStatusInputs(
                deck_path=deck_path,
                pdf_path=pdf_path,
                slides_dir=slides_dir,
                montage_path=montage_path,
                output_path=output_path,
                snapshot_date=resolved_snapshot_date,
                source_run_id=resolved_run_id,
                deck_slide_count=deck_slide_count,
                expected_slide_count=expected,
                rendered_slide_paths=rendered_slide_paths,
                findings=findings,
            )
        ),
        output_path=output_path,
        render_dir=render_dir,
    )


class RenderStatusInputs:
    def __init__(
        self,
        *,
        deck_path: Path,
        pdf_path: Path,
        slides_dir: Path,
        montage_path: Path,
        output_path: Path | None,
        snapshot_date: str,
        source_run_id: str,
        deck_slide_count: int,
        expected_slide_count: int,
        rendered_slide_paths: list[Path],
        findings: list[dict[str, Any]],
    ) -> None:
        self.deck_path = deck_path
        self.pdf_path = pdf_path
        self.slides_dir = slides_dir
        self.montage_path = montage_path
        self.output_path = output_path
        self.snapshot_date = snapshot_date
        self.source_run_id = source_run_id
        self.deck_slide_count = deck_slide_count
        self.expected_slide_count = expected_slide_count
        self.rendered_slide_paths = rendered_slide_paths
        self.findings = findings


def _deck_slide_count(deck_path: Path, findings: list[dict[str, Any]]) -> int:
    if not deck_path.exists():
        return 0
    try:
        return len(Presentation(str(deck_path)).slides)
    except Exception as exc:
        findings.append(_finding("high", "deck_load_failed", str(exc)))
        return 0


def _output_dir(
    *,
    output_path: Path | None,
    output_root: Path,
    snapshot_date: str,
    source_run_id: str,
) -> Path:
    if output_path:
        return Path(output_path).parent
    return Path(output_root) / snapshot_date / source_run_id


def _clean_render_targets(*, pdf_path: Path, slides_dir: Path, montage_path: Path) -> None:
    pdf_path.unlink(missing_ok=True)
    montage_path.unlink(missing_ok=True)
    for png_path in slides_dir.glob("*.png"):
        png_path.unlink()


def _render_pdf(
    *,
    deck_path: Path,
    pdf_path: Path,
    timeout_seconds: int,
    findings: list[dict[str, Any]],
) -> None:
    soffice = shutil.which("soffice")
    if not soffice:
        findings.append(_finding("high", "soffice_missing", "soffice not found on PATH"))
        return
    run = subprocess.run(
        [
            soffice,
            "--headless",
            "--convert-to",
            "pdf",
            "--outdir",
            str(pdf_path.parent),
            str(deck_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    if run.returncode != 0:
        findings.append(
            _finding(
                "high",
                "pptx_to_pdf_render_failed",
                run.stderr.strip() or run.stdout.strip() or f"returncode={run.returncode}",
            )
        )
        return
    converted_pdf = pdf_path.parent / f"{deck_path.stem}.pdf"
    if converted_pdf != pdf_path and converted_pdf.exists():
        converted_pdf.replace(pdf_path)
    if not pdf_path.exists():
        findings.append(_finding("high", "pdf_missing", f"Expected PDF at {pdf_path}"))


def _render_pngs(
    *,
    pdf_path: Path,
    slides_dir: Path,
    timeout_seconds: int,
    findings: list[dict[str, Any]],
) -> None:
    pdftoppm = shutil.which("pdftoppm")
    if not pdftoppm:
        findings.append(_finding("high", "pdftoppm_missing", "pdftoppm not found on PATH"))
        return
    run = subprocess.run(
        [
            pdftoppm,
            "-png",
            str(pdf_path),
            str(slides_dir / "slide"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    if run.returncode != 0:
        findings.append(
            _finding(
                "high",
                "pdf_to_png_render_failed",
                run.stderr.strip() or run.stdout.strip() or f"returncode={run.returncode}",
            )
        )


def _validate_render_outputs(
    *,
    pdf_path: Path,
    slide_paths: list[Path],
    expected_slide_count: int,
    findings: list[dict[str, Any]],
) -> None:
    if pdf_path.stat().st_size < MIN_PDF_BYTES:
        findings.append(
            _finding(
                "high",
                "pdf_file_size_suspiciously_small",
                f"{pdf_path.stat().st_size} bytes",
            )
        )
    if len(slide_paths) != expected_slide_count:
        findings.append(
            _finding(
                "high",
                "rendered_slide_count_mismatch",
                f"{len(slide_paths)} rendered slides; expected {expected_slide_count}",
            )
        )
    for index, slide_path in enumerate(slide_paths, start=1):
        _validate_slide_image(index=index, slide_path=slide_path, findings=findings)


def _validate_slide_image(
    *,
    index: int,
    slide_path: Path,
    findings: list[dict[str, Any]],
) -> None:
    if slide_path.stat().st_size < MIN_PNG_BYTES:
        findings.append(
            _finding(
                "high",
                "rendered_slide_too_small",
                f"slide {index}: {slide_path.stat().st_size} bytes",
                slide=index,
            )
        )
        return
    try:
        with Image.open(slide_path) as image:
            rgb = image.convert("RGB")
            stat = ImageStat.Stat(rgb)
            inverted_bbox = ImageChops.invert(rgb).getbbox()
            channel_stddev = sum(stat.stddev) / len(stat.stddev)
            if rgb.width < 500 or rgb.height < 300:
                findings.append(
                    _finding(
                        "high",
                        "rendered_slide_dimensions_too_small",
                        f"slide {index}: {rgb.width}x{rgb.height}",
                        slide=index,
                    )
                )
            if inverted_bbox is None or channel_stddev < MIN_IMAGE_STDDEV:
                findings.append(
                    _finding(
                        "high",
                        "rendered_slide_blank_or_flat",
                        f"slide {index}: stddev={channel_stddev:.2f}",
                        slide=index,
                    )
                )
    except Exception as exc:
        findings.append(
            _finding(
                "high",
                "rendered_slide_load_failed",
                f"slide {index}: {exc}",
                slide=index,
            )
        )


def _build_montage(slide_paths: list[Path], montage_path: Path) -> None:
    images = [Image.open(path).convert("RGB") for path in slide_paths]
    try:
        thumb_width = 420
        thumb_height = 236
        padding = 20
        label_height = 28
        columns = min(3, len(images))
        rows = (len(images) + columns - 1) // columns
        canvas = Image.new(
            "RGB",
            (
                columns * thumb_width + (columns + 1) * padding,
                rows * (thumb_height + label_height) + (rows + 1) * padding,
            ),
            (246, 247, 249),
        )
        draw = ImageDraw.Draw(canvas)
        for index, image in enumerate(images):
            row = index // columns
            col = index % columns
            x = padding + col * (thumb_width + padding)
            y = padding + row * (thumb_height + label_height + padding)
            thumbnail = image.copy()
            thumbnail.thumbnail((thumb_width, thumb_height))
            frame = Image.new("RGB", (thumb_width, thumb_height), "white")
            frame.paste(
                thumbnail,
                (
                    (thumb_width - thumbnail.width) // 2,
                    (thumb_height - thumbnail.height) // 2,
                ),
            )
            canvas.paste(frame, (x, y + label_height))
            draw.text((x, y), f"Slide {index + 1}", fill=(30, 30, 30))
        montage_path.parent.mkdir(parents=True, exist_ok=True)
        canvas.save(montage_path)
    finally:
        for image in images:
            image.close()


def _slide_number(path: Path) -> int:
    token = path.stem.split("-")[-1]
    try:
        return int(token)
    except ValueError:
        return 0


def _result_payload(*, status_inputs: RenderStatusInputs) -> dict[str, Any]:
    findings = status_inputs.findings
    high_count = sum(1 for finding in findings if finding["severity"] == "high")
    medium_count = sum(1 for finding in findings if finding["severity"] == "medium")
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if high_count else "ok",
        "snapshot_date": status_inputs.snapshot_date,
        "source_run_id": status_inputs.source_run_id,
        "deck_path": str(status_inputs.deck_path),
        "pdf_path": str(status_inputs.pdf_path) if status_inputs.pdf_path.exists() else None,
        "slides_dir": str(status_inputs.slides_dir),
        "montage_path": (
            str(status_inputs.montage_path)
            if status_inputs.montage_path.exists()
            else None
        ),
        "checks": {
            "deck_slide_count": status_inputs.deck_slide_count,
            "expected_slide_count": status_inputs.expected_slide_count,
            "rendered_slide_count": len(status_inputs.rendered_slide_paths),
            "pdf_file_size_bytes": (
                status_inputs.pdf_path.stat().st_size
                if status_inputs.pdf_path.exists()
                else 0
            ),
            "rendered_png_checked": bool(status_inputs.rendered_slide_paths),
            "rendered_slide_paths": [str(path) for path in status_inputs.rendered_slide_paths],
        },
        "summary": {
            "finding_count": len(findings),
            "high_finding_count": high_count,
            "medium_finding_count": medium_count,
            "low_finding_count": sum(
                1 for finding in findings if finding["severity"] == "low"
            ),
        },
        "findings": findings,
    }


def _write_result(
    *,
    payload: dict[str, Any],
    output_path: Path | None,
    render_dir: Path,
) -> dict[str, Any]:
    if output_path is None:
        output_path = render_dir / "source_backed_deck_render_audit.json"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload["output_path"] = str(output_path)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload


def _finding(
    severity: str,
    issue: str,
    evidence: str,
    *,
    slide: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "severity": severity,
        "issue": issue,
        "evidence": evidence,
    }
    if slide is not None:
        payload["slide"] = slide
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deck-path", type=Path, required=True)
    parser.add_argument("--snapshot-date")
    parser.add_argument("--source-run-id")
    parser.add_argument("--expected-slide-count", type=int)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = validate_deck_render(
        deck_path=args.deck_path,
        output_path=args.output_path,
        output_root=args.output_root,
        expected_slide_count=args.expected_slide_count,
        snapshot_date=args.snapshot_date,
        source_run_id=args.source_run_id,
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
