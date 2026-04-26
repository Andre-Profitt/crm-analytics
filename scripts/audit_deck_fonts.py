#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "output" / "deck_font_audit"
DECKS_ROOT = ROOT / "output" / "simcorp_director_decks"
SLIDES_SKILL_SCRIPTS = Path.home() / ".codex" / "skills" / "slides" / "scripts"
DETECT_FONT_SCRIPT = SLIDES_SKILL_SCRIPTS / "detect_font.py"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _decks_dir(run_date: str) -> Path:
    return DECKS_ROOT / str(run_date)[:10] / "land-only"


def _run_detect_font(deck_path: Path) -> dict[str, Any]:
    run = subprocess.run(
        [
            sys.executable,
            str(DETECT_FONT_SCRIPT),
            "--json",
            str(deck_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(run.stdout or "{}")
    if not isinstance(payload, dict):
        raise ValueError(f"{deck_path}: expected JSON object from detect_font")
    return payload


def audit_decks(run_date: str, decks_dir: Path) -> dict[str, Any]:
    decks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    warning_count = 0
    for deck_path in sorted(decks_dir.glob("*.pptx")):
        if deck_path.name.startswith("~"):
            continue
        deck_name = deck_path.stem
        try:
            font_report = _run_detect_font(deck_path)
        except Exception as exc:
            failures.append(
                {
                    "deck": deck_name,
                    "deck_path": _display_path(deck_path),
                    "issue": "font_audit_failed",
                    "message": str(exc),
                }
            )
            continue
        missing_overall = sorted(set(font_report.get("font_missing_overall") or []))
        substituted_overall = sorted(
            set(font_report.get("font_substituted_overall") or [])
        )
        issue_count = len(missing_overall) + len(substituted_overall)
        if issue_count:
            warning_count += 1
        decks.append(
            {
                "deck": deck_name,
                "deck_path": _display_path(deck_path),
                "font_missing_overall": missing_overall,
                "font_missing_by_slide": dict(font_report.get("font_missing_by_slide") or {}),
                "font_substituted_overall": substituted_overall,
                "font_substituted_by_slide": dict(
                    font_report.get("font_substituted_by_slide") or {}
                ),
                "font_missing_count": len(missing_overall),
                "font_substituted_count": len(substituted_overall),
                "has_issues": bool(issue_count),
            }
        )

    status = "failed" if failures else ("warning" if warning_count else "ok")
    return {
        "run_date": str(run_date)[:10],
        "decks_dir": _display_path(decks_dir),
        "status": status,
        "deck_count": len(decks),
        "decks_with_issues": warning_count,
        "decks": decks,
        "failures": failures,
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        f"# Deck Font Audit — {payload['run_date']}",
        "",
        f"- Status: `{payload['status']}`",
        f"- Decks audited: `{payload['deck_count']}`",
        f"- Decks with font issues: `{payload['decks_with_issues']}`",
        f"- Failures: `{len(payload['failures'])}`",
        "",
        "## Decks",
        "",
    ]
    for item in payload.get("decks") or []:
        if item["has_issues"]:
            lines.append(
                f"- `{item['deck']}`: missing `{item['font_missing_overall'] or []}`, substituted `{item['font_substituted_overall'] or []}`"
            )
        else:
            lines.append(f"- `{item['deck']}`: clean")
    lines.extend(["", "## Failures", ""])
    if payload["failures"]:
        for failure in payload["failures"]:
            lines.append(
                f"- `{failure['deck']}`: `{failure['issue']}` -> `{failure['message']}`"
            )
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
        default=None,
        help="Optional decks directory override. Defaults to output/simcorp_director_decks/<date>/land-only.",
    )
    args = parser.parse_args()

    run_date = str(args.date)[:10]
    decks_dir = Path(args.decks_dir) if args.decks_dir else _decks_dir(run_date)
    if not decks_dir.exists():
        print(f"Deck directory missing: {decks_dir}", file=sys.stderr)
        return 1

    payload = audit_decks(run_date, decks_dir)
    output_dir = OUTPUT_ROOT / run_date
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "deck_font_audit.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)
    print(f"Deck font audit: {payload['status']}")
    print(f"Output: {_display_path(output_dir)}")
    return 1 if payload["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
