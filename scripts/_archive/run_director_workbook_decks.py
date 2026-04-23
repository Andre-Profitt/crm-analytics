#!/usr/bin/env python3
"""Build workbook-native Sales Director decks from corrected Excel workbooks."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from extract_director_workbook_snapshot import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SNAPSHOT_ROOT,
    DEFAULT_WORKBOOK_ROOT,
    run as extract_snapshots,
    slugify,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = REPO_ROOT / "output" / "director_workbook_deck_2026-04-10"
BUILDER = WORKSPACE / "build_director_workbook_deck.js"
DEFAULT_DECK_ROOT = REPO_ROOT / "output" / "director_workbook_deck_runs"
MONTAGE_SCRIPT = (
    REPO_ROOT
    / "output"
    / "sales_director_monthly_deck_2026-03-31"
    / "scripts"
    / "create_montage.py"
)


def run_cmd(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd or REPO_ROOT, capture_output=True, text=True, check=True)


def render_with_libreoffice(deck_path: Path, run_dir: Path) -> dict[str, Any]:
    pdf_dir = run_dir / "libreoffice"
    png_dir = run_dir / f"{deck_path.stem}_rendered"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    png_dir.mkdir(parents=True, exist_ok=True)
    run_cmd(
        [
            "soffice",
            "--headless",
            "--convert-to",
            "pdf",
            "--outdir",
            str(pdf_dir),
            str(deck_path),
        ]
    )
    pdf_path = pdf_dir / f"{deck_path.stem}.pdf"
    run_cmd(["pdftoppm", "-png", str(pdf_path), str(png_dir / "slide")])
    montage_path = run_dir / f"{deck_path.stem}_montage.png"
    run_cmd(
        [
            "python3",
            str(MONTAGE_SCRIPT),
            "--input_dir",
            str(png_dir),
            "--output_file",
            str(montage_path),
        ]
    )
    return {
        "pdf_path": str(pdf_path),
        "rendered_dir": str(png_dir),
        "montage_path": str(montage_path),
    }


def build_deck(snapshot_path: Path, deck_root: Path, render: bool) -> dict[str, Any]:
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    slug = slugify(snapshot["director_name"])
    run_dir = deck_root / snapshot["snapshot_date"]
    run_dir.mkdir(parents=True, exist_ok=True)
    deck_path = run_dir / f"{slug}.pptx"
    summary_path = run_dir / f"{slug}.summary.json"
    proc = run_cmd(
        [
            "node",
            str(BUILDER),
            "--snapshot",
            str(snapshot_path),
            "--output",
            str(deck_path),
            "--summary-json",
            str(summary_path),
        ]
    )
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    result = {
        "director_name": snapshot["director_name"],
        "territory": snapshot["territory"],
        "snapshot_path": str(snapshot_path),
        "deck_path": str(deck_path),
        "summary_path": str(summary_path),
        "slide_count": payload.get("slide_count"),
        "builder_stdout": proc.stdout.strip(),
        "builder_stderr": proc.stderr.strip(),
    }
    if render:
        result["render"] = render_with_libreoffice(deck_path, run_dir)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", default="2026-04-10")
    parser.add_argument("--director")
    parser.add_argument("--workbook-root", type=Path, default=DEFAULT_WORKBOOK_ROOT)
    parser.add_argument("--snapshot-root", type=Path, default=DEFAULT_SNAPSHOT_ROOT)
    parser.add_argument("--deck-root", type=Path, default=DEFAULT_DECK_ROOT)
    parser.add_argument("--render", action="store_true", help="Render a LibreOffice PDF + montage.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    snapshot_paths = extract_snapshots(
        args.snapshot_date,
        args.director,
        args.workbook_root,
        args.snapshot_root,
    )
    builds = [build_deck(path, args.deck_root, args.render) for path in snapshot_paths]
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_date": args.snapshot_date,
        "director": args.director,
        "build_count": len(builds),
        "builds": builds,
    }
    manifest_path = args.deck_root / args.snapshot_date / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
