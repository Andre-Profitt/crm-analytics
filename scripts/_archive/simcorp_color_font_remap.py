#!/usr/bin/env python3
"""Remap canonical-deck colors and fonts to production SimCorp values
in-place inside a transplanted pptx, by walking the slide XML files.

Color mapping derived from comparing the canonical 812 KB deck's used
fills/text colors against the production SimCorp 'Commercial Update' and
'QBR' reference decks. Font mapping replaces the Apple-system 'Avenir Next'
with the SimCorp body font 'Microsoft Sans Serif'.

Run:
    python3 scripts/simcorp_color_font_remap.py <input.pptx> <output.pptx>
"""

from __future__ import annotations

import re
import shutil
import sys
import zipfile
from pathlib import Path

# canonical hex (uppercase, no #) -> production SimCorp hex
COLOR_MAP = {
    "0A6C74": "0E3788",  # canonical primary teal -> SimCorp primary blue
    "0A4D57": "011946",  # canonical dark teal    -> SimCorp dark navy
    "B45A43": "9D2E7B",  # canonical rust accent  -> SimCorp magenta
    "A7852C": "9D2E7B",  # canonical gold accent  -> SimCorp magenta
    "EEF3F5": "E6EEFE",  # canonical light teal panel -> light blue panel
    "FBF8F2": "FFFFFF",  # canonical cream bg     -> white
    "F7FAFB": "FFFFFF",  # canonical cool white   -> white
    "DCEEF0": "6FCCDD",  # canonical light cyan   -> SimCorp aqua
    "F7E3DD": "E6EEFE",  # canonical light rust   -> light blue panel
    "F4E8BF": "E6EEFE",  # canonical light gold   -> light blue panel
    "E3F0E7": "E6EEFE",  # canonical light green  -> light blue panel
    "CDE7EA": "6FCCDD",  # canonical light cyan text -> SimCorp aqua
    "123040": "011946",  # canonical dark navy text -> SimCorp dark navy
    "5C7482": "0E3788",  # canonical secondary text -> SimCorp primary blue
    "0F2430": "011946",  # canonical dark text    -> SimCorp dark navy
}

# font replacements (case-insensitive)
FONT_MAP = {
    "Avenir Next": "Microsoft Sans Serif",
}


def remap_xml(xml: bytes) -> tuple[bytes, dict]:
    """Apply color and font remaps to a slide XML payload."""
    text = xml.decode("utf-8")
    counts = {"colors": 0, "fonts": 0}

    # Color remap: hex appears in val="XXXXXX" attributes (uppercase)
    # We do a case-insensitive match on the hex pair to handle either case.
    for old_hex, new_hex in COLOR_MAP.items():
        # Match val="0a6c74" or val="0A6C74"
        pattern = re.compile(rf'val="({old_hex})"', re.IGNORECASE)
        new_text, n = pattern.subn(f'val="{new_hex}"', text)
        if n:
            text = new_text
            counts["colors"] += n

    # Font remap: typeface attribute on rPr or other run-property elements
    for old_font, new_font in FONT_MAP.items():
        pattern = re.compile(rf'typeface="{re.escape(old_font)}"', re.IGNORECASE)
        new_text, n = pattern.subn(f'typeface="{new_font}"', text)
        if n:
            text = new_text
            counts["fonts"] += n

    return text.encode("utf-8"), counts


def main():
    if len(sys.argv) < 3:
        print("Usage: simcorp_color_font_remap.py <input.pptx> <output.pptx>")
        sys.exit(1)

    src_path = Path(sys.argv[1])
    dst_path = Path(sys.argv[2])

    print(f"Source: {src_path}")
    print(f"Output: {dst_path}")

    # Copy then patch in place
    shutil.copy2(src_path, dst_path)

    total = {"colors": 0, "fonts": 0}
    slides_touched = 0

    with zipfile.ZipFile(dst_path, "a") as zf:
        slide_names = [
            n
            for n in zf.namelist()
            if n.startswith("ppt/slides/slide") and n.endswith(".xml")
        ]
        # Also patch slide layouts and masters in case they have shared theme bits
        layout_names = [
            n
            for n in zf.namelist()
            if n.startswith("ppt/slideLayouts/") and n.endswith(".xml")
        ]
        all_targets = sorted(slide_names) + sorted(layout_names)

    # Re-open in write-mode by re-creating the zip
    tmp_path = dst_path.with_suffix(".tmp.pptx")
    with zipfile.ZipFile(dst_path, "r") as zin:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename in all_targets:
                    new_data, c = remap_xml(data)
                    if c["colors"] or c["fonts"]:
                        slides_touched += 1
                        total["colors"] += c["colors"]
                        total["fonts"] += c["fonts"]
                        if "ppt/slides/slide" in item.filename:
                            print(
                                f"  {item.filename}: {c['colors']} colors, {c['fonts']} fonts"
                            )
                    data = new_data
                zout.writestr(item, data)

    tmp_path.replace(dst_path)
    print()
    print(f"Files patched: {slides_touched}")
    print(f"Total color swaps: {total['colors']}")
    print(f"Total font swaps:  {total['fonts']}")
    print(f"Saved: {dst_path}")


if __name__ == "__main__":
    main()
