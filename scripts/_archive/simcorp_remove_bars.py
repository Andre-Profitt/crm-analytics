#!/usr/bin/env python3
"""Strip the top/bottom/left decorative bars from a SimCorp-transplanted pptx.

- Sets showMasterSp="0" on every content slide (hides master shapes:
  title bar area at top, footer logo + page number at bottom).
- Removes the left vertical bar PICTURE shape from the Blank slide layout
  so slides using that layout render on a clean canvas.
- Leaves slide 1 alone (uses SC-Master Gradient_Title which has its own
  intentional chrome).

Run:
    python3 scripts/simcorp_remove_bars.py <input.pptx> <output.pptx>
"""

from __future__ import annotations

import re
import shutil
import sys
import zipfile
from pathlib import Path


def strip_layout_pictures(xml: str) -> str:
    """Remove <p:pic>...</p:pic> elements from a slideLayout XML."""
    return re.sub(r"<p:pic>.*?</p:pic>", "", xml, flags=re.DOTALL)


def add_show_master_sp_false(xml: str) -> str:
    """Add showMasterSp='0' attribute to <p:sld> root element."""
    # Look for <p:sld ...> or <p:sld>
    # Already present?
    if 'showMasterSp="0"' in xml:
        return xml
    # Inject the attribute into the opening <p:sld ...> tag
    return re.sub(
        r"(<p:sld\b)([^>]*)>",
        r'\1\2 showMasterSp="0">',
        xml,
        count=1,
    )


def main():
    if len(sys.argv) < 3:
        print("Usage: simcorp_remove_bars.py <input.pptx> <output.pptx>")
        sys.exit(1)

    src = Path(sys.argv[1])
    dst = Path(sys.argv[2])
    print(f"Source: {src}")
    print(f"Output: {dst}")

    # Copy then rewrite
    shutil.copy2(src, dst)
    tmp = dst.with_suffix(".working.pptx")

    BLANK_LAYOUT_RE = re.compile(rb'name="Blank"')
    slides_patched = 0
    layouts_patched = 0

    with zipfile.ZipFile(dst, "r") as zin:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename.startswith(
                    "ppt/slides/slide"
                ) and item.filename.endswith(".xml"):
                    # Skip slide 1 (cover)
                    m = re.search(r"slide(\d+)\.xml", item.filename)
                    slide_num = int(m.group(1)) if m else -1
                    if slide_num != 1:
                        text = data.decode("utf-8")
                        new_text = add_show_master_sp_false(text)
                        if new_text != text:
                            slides_patched += 1
                            data = new_text.encode("utf-8")
                elif item.filename.startswith(
                    "ppt/slideLayouts/"
                ) and item.filename.endswith(".xml"):
                    # Only patch the Blank layout
                    if BLANK_LAYOUT_RE.search(data):
                        text = data.decode("utf-8")
                        new_text = strip_layout_pictures(text)
                        if new_text != text:
                            layouts_patched += 1
                            data = new_text.encode("utf-8")
                            print(f"  Stripped pictures from {item.filename}")
                zout.writestr(item, data)

    tmp.replace(dst)
    print()
    print(f"Slides with showMasterSp=0 applied: {slides_patched}")
    print(f"Layouts stripped of pictures:        {layouts_patched}")
    print(f"Saved: {dst}")


if __name__ == "__main__":
    main()
