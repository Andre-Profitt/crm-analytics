#!/usr/bin/env python3
"""Polish pass on a SimCorp-transplanted pptx:

1. Remove the full-width top bar rectangle from each content slide.
   Fingerprint: <p:sp> with offset (0, 0) and extent (full slide width,
   height <= 0.3 inches).
2. Strip fontScale and lnSpcReduction from <a:normAutofit> elements so
   text renders at its natural size instead of being shrunk/truncated.

Run:
    python3 scripts/simcorp_polish.py <input.pptx> <output.pptx>
"""

from __future__ import annotations

import re
import shutil
import sys
import zipfile
from pathlib import Path

from lxml import etree  # noqa: F401

# OOXML namespaces
NS = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

# Full slide width in EMU (13.333 in * 914400)
SLIDE_WIDTH_EMU = 12192000
SLIDE_WIDTH_TOL = 300000  # ~0.33 in tolerance

# Top bar height threshold (0.3 in)
MAX_BAR_HEIGHT_EMU = 300000


def remove_top_bar_from_slide(xml: bytes) -> tuple[bytes, int]:
    """Remove full-width shapes at (0,0) with height <= 0.3 in from the slide."""
    root = etree.fromstring(xml)
    removed = 0

    # The shape tree is under p:cSld/p:spTree
    sp_tree = root.find(".//p:cSld/p:spTree", NS)
    if sp_tree is None:
        return xml, 0

    shapes_to_remove = []
    for sp in list(sp_tree):
        tag = sp.tag.split("}")[-1]
        if tag not in ("sp", "pic"):
            continue

        # Find the xfrm (transform) element inside this shape
        xfrm = sp.find(".//p:spPr/a:xfrm", NS)
        if xfrm is None:
            continue

        off = xfrm.find("a:off", NS)
        ext = xfrm.find("a:ext", NS)
        if off is None or ext is None:
            continue

        try:
            x = int(off.get("x", "0"))
            y = int(off.get("y", "0"))
            cx = int(ext.get("cx", "0"))
            cy = int(ext.get("cy", "0"))
        except ValueError:
            continue

        # Full-width at origin with small height
        if (
            abs(x) <= 100000  # within 0.1 in of left edge
            and abs(y) <= 100000  # within 0.1 in of top edge
            and cx >= (SLIDE_WIDTH_EMU - SLIDE_WIDTH_TOL)
            and cy <= MAX_BAR_HEIGHT_EMU
        ):
            shapes_to_remove.append(sp)

    for sp in shapes_to_remove:
        sp_tree.remove(sp)
        removed += 1

    if removed == 0:
        return xml, 0

    new_xml = etree.tostring(
        root, xml_declaration=True, encoding="UTF-8", standalone=True
    )
    return new_xml, removed


def strip_autofit_scaling(xml: bytes) -> tuple[bytes, int]:
    """Strip fontScale and lnSpcReduction attrs from <a:normAutofit/> so text
    renders at natural size."""
    text = xml.decode("utf-8")
    # Remove fontScale
    new_text, n1 = re.subn(r'(<a:normAutofit\b[^/]*?)\s*fontScale="\d+"', r"\1", text)
    # Remove lnSpcReduction
    new_text, n2 = re.subn(
        r'(<a:normAutofit\b[^/]*?)\s*lnSpcReduction="\d+"', r"\1", new_text
    )
    total = n1 + n2
    return new_text.encode("utf-8"), total


def main():
    if len(sys.argv) < 3:
        print("Usage: simcorp_polish.py <input.pptx> <output.pptx>")
        sys.exit(1)

    src = Path(sys.argv[1])
    dst = Path(sys.argv[2])
    print(f"Source: {src}")
    print(f"Output: {dst}")

    shutil.copy2(src, dst)
    tmp = dst.with_name(dst.stem + "_working.pptx")

    total_bars = 0
    total_autofit = 0

    with zipfile.ZipFile(dst, "r") as zin:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)

                if item.filename.startswith(
                    "ppt/slides/slide"
                ) and item.filename.endswith(".xml"):
                    m = re.search(r"slide(\d+)\.xml", item.filename)
                    slide_num = int(m.group(1)) if m else -1
                    # Skip slide 1 (cover uses SC-Master Gradient_Title)
                    if slide_num != 1:
                        data, bars = remove_top_bar_from_slide(data)
                        data, autofit = strip_autofit_scaling(data)
                        if bars or autofit:
                            print(
                                f"  {item.filename}: removed {bars} top bars, stripped {autofit} autofit scalings"
                            )
                            total_bars += bars
                            total_autofit += autofit

                zout.writestr(item, data)

    tmp.replace(dst)
    print()
    print(f"Total top bars removed:    {total_bars}")
    print(f"Total autofit scalings stripped: {total_autofit}")
    print(f"Saved: {dst}")


if __name__ == "__main__":
    main()
