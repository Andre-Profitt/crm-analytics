#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DECKS_ROOT = ROOT / "output" / "simcorp_director_decks"
OUTPUT_ROOT = ROOT / "output" / "deck_font_normalization"

CALIBRI_TYPEFACE_RE = re.compile(rb'typeface=(["\'])Calibri\1', re.IGNORECASE)
ARIAL_TYPEFACE = b'typeface="Arial"'


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _normalize_pptx(path: Path) -> dict[str, Any]:
    replacement_count = 0
    modified_parts: list[str] = []
    with tempfile.NamedTemporaryFile(
        suffix=".pptx",
        prefix=f"{path.stem}-font-normalize-",
        dir=path.parent,
        delete=False,
    ) as handle:
        temp_path = Path(handle.name)

    try:
        with ZipFile(path, "r") as src, ZipFile(
            temp_path, "w", compression=ZIP_DEFLATED
        ) as dst:
            for info in src.infolist():
                payload = src.read(info.filename)
                if info.filename.endswith(".xml"):
                    updated, replacements = CALIBRI_TYPEFACE_RE.subn(
                        ARIAL_TYPEFACE, payload
                    )
                    if replacements:
                        payload = updated
                        replacement_count += replacements
                        modified_parts.append(info.filename)
                dst.writestr(info, payload)
        if replacement_count:
            shutil.move(temp_path, path)
        else:
            temp_path.unlink(missing_ok=True)
        return {
            "deck_name": path.name,
            "deck_path": _display_path(path),
            "status": "modified" if replacement_count else "unchanged",
            "replacement_count": replacement_count,
            "modified_parts": modified_parts,
        }
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        f"# Deck Font Normalization — {payload['run_date']}",
        "",
        f"- Status: `{payload['status']}`",
        f"- Decks scanned: `{payload['deck_count']}`",
        f"- Decks modified: `{payload['modified_deck_count']}`",
        f"- Replacements: `{payload['replacement_count']}`",
        f"- Failures: `{len(payload['failures'])}`",
        "",
        "## Decks",
        "",
    ]
    if payload["decks"]:
        for deck in payload["decks"]:
            if deck["status"] == "modified":
                lines.append(
                    f"- `{deck['deck_name']}`: `{deck['replacement_count']}` replacement(s)"
                )
            else:
                lines.append(f"- `{deck['deck_name']}`: unchanged")
    else:
        lines.append("- none")

    lines.extend(["", "## Failures", ""])
    if payload["failures"]:
        for failure in payload["failures"]:
            lines.append(f"- `{failure['deck_name']}`: {failure['message']}")
    else:
        lines.append("- none")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--decks-dir",
        type=Path,
        default=None,
        help="Defaults to output/simcorp_director_decks/<date>/land-only.",
    )
    args = parser.parse_args()

    run_date = str(args.date)[:10]
    decks_dir = args.decks_dir or (DEFAULT_DECKS_ROOT / run_date / "land-only")
    output_dir = OUTPUT_ROOT / run_date
    output_dir.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "run_date": run_date,
        "decks_dir": _display_path(decks_dir),
        "status": "ok",
        "deck_count": 0,
        "modified_deck_count": 0,
        "replacement_count": 0,
        "decks": [],
        "failures": [],
    }

    if not decks_dir.exists():
        payload["status"] = "failed"
        payload["failures"].append(
            {
                "deck_name": None,
                "message": f"missing {decks_dir}",
            }
        )
    else:
        for deck_path in sorted(decks_dir.glob("*.pptx")):
            if deck_path.name.startswith("~"):
                continue
            payload["deck_count"] += 1
            try:
                deck_payload = _normalize_pptx(deck_path)
                payload["decks"].append(deck_payload)
                if deck_payload["status"] == "modified":
                    payload["modified_deck_count"] += 1
                    payload["replacement_count"] += int(
                        deck_payload["replacement_count"]
                    )
            except Exception as exc:
                payload["status"] = "failed"
                payload["failures"].append(
                    {
                        "deck_name": deck_path.name,
                        "message": str(exc),
                    }
                )

    payload_path = output_dir / "deck_font_normalization.json"
    summary_path = output_dir / "summary.md"
    payload_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_summary(summary_path, payload)

    print(f"Deck font normalization: {payload['status']}")
    print(f"Output: {output_dir}")
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
