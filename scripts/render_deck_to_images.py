#!/usr/bin/env python3
"""Track F / F6 — render a deck to per-slide PNGs.

Pipeline: .pptx -> PDF (libreoffice headless) -> per-slide PNG (pdftoppm).
Pure shellouts; no python-pptx rendering needed.

Usage:
    python3 scripts/render_deck_to_images.py \\
        --pptx ~/Downloads/jesper-tyrer-LAND.pptx \\
        --out /tmp/jesper-tyrer-LAND-png \\
        --dpi 150
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def render_to_pngs(
    pptx: Path,
    out_dir: Path,
    *,
    dpi: int = 150,
    soffice: str = "/opt/homebrew/bin/soffice",
) -> list[Path]:
    """Render every slide of the pptx to PNGs in out_dir.

    Returns ordered list of slide-N.png paths (1-indexed).
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # soffice writes PDF next to the input by default; redirect with --outdir.
    subprocess.run(
        [
            soffice,
            "--headless",
            "--convert-to",
            "pdf",
            "--outdir",
            str(out_dir),
            str(pptx),
        ],
        check=True,
        capture_output=True,
    )
    pdf = out_dir / (pptx.stem + ".pdf")
    if not pdf.exists():
        raise RuntimeError(f"soffice did not produce {pdf}")

    # pdftoppm: write slide-NNN.png files.
    prefix = out_dir / "slide"
    subprocess.run(
        ["pdftoppm", "-png", "-r", str(dpi), str(pdf), str(prefix)],
        check=True,
        capture_output=True,
    )

    # pdftoppm writes slide-1.png, slide-2.png, ... (or slide-01.png if zero-padded
    # on some versions). Glob both and sort numerically.
    pngs = sorted(out_dir.glob("slide-*.png"), key=lambda p: int(p.stem.split("-")[-1]))
    if not pngs:
        raise RuntimeError(f"pdftoppm produced no pngs in {out_dir}")
    return pngs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render PPTX to per-slide PNGs.")
    parser.add_argument("--pptx", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--dpi", type=int, default=150)
    parser.add_argument("--soffice", default="/opt/homebrew/bin/soffice")
    parser.add_argument("--keep-pdf", action="store_true")
    args = parser.parse_args(argv)

    if not args.pptx.exists():
        print(f"ERROR: pptx not found: {args.pptx}", file=sys.stderr)
        return 2
    pngs = render_to_pngs(args.pptx, args.out, dpi=args.dpi, soffice=args.soffice)
    if not args.keep_pdf:
        pdf = args.out / (args.pptx.stem + ".pdf")
        if pdf.exists():
            pdf.unlink()
    for p in pngs:
        print(p)
    print(f"rendered {len(pngs)} slides to {args.out}/", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
