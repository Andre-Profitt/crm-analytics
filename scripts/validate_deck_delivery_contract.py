#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from pptx import Presentation


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "output" / "deck_delivery_contract"
DIRECTOR_MIN_SLIDES = 5
EXEC_ROLLUP_MIN_SLIDES = 3
DIRECTOR_SIDECAR_FIELDS = [
    "director",
    "territory",
    "open_land_deals",
    "open_land_arr",
    "q1_land_wins",
    "q1_land_wins_arr",
    "q1_land_lost",
    "q1_land_lost_arr",
    "q2_renewals",
    "q2_renewals_acv",
    "approved_2026",
    "conditionally_approved",
    "missing_stage3",
]


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _infer_report_date(
    workbooks_dir: Path | None, decks_dir: Path | None, report_date: str | None
) -> str:
    token = str(report_date or "").strip()[:10]
    if token:
        return token
    for root in [workbooks_dir, decks_dir]:
        if root is None:
            continue
        for candidate in [Path(root), *Path(root).parents]:
            try:
                datetime.strptime(candidate.name, "%Y-%m-%d")
                return candidate.name
            except ValueError:
                continue
    return datetime.now().strftime("%Y-%m-%d")


def _director_slugs(workbooks_dir: Path) -> list[str]:
    return sorted(
        path.stem
        for path in workbooks_dir.glob("*.xlsx")
        if not path.name.startswith("~")
    )


def _presentation_slide_count(path: Path) -> int:
    prs = Presentation(str(path))
    return len(prs.slides)


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _director_sidecar_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: payload.get(key) for key in DIRECTOR_SIDECAR_FIELDS}


def _validate_director_deck(
    *,
    slug: str,
    decks_dir: Path,
    validated: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    deck_path = decks_dir / f"{slug}-LAND.pptx"
    sidecar_path = decks_dir / f"{slug}-LAND.json"
    if not deck_path.exists():
        failures.append(
            {
                "scope": "director",
                "slug": slug,
                "issue": "missing_deck",
                "message": f"missing {deck_path}",
            }
        )
        return
    if not sidecar_path.exists():
        failures.append(
            {
                "scope": "director",
                "slug": slug,
                "issue": "missing_sidecar",
                "message": f"missing {sidecar_path}",
            }
        )
        return

    try:
        slide_count = _presentation_slide_count(deck_path)
    except Exception as exc:
        failures.append(
            {
                "scope": "director",
                "slug": slug,
                "issue": "deck_load_failed",
                "message": str(exc),
            }
        )
        return
    if slide_count < DIRECTOR_MIN_SLIDES:
        failures.append(
            {
                "scope": "director",
                "slug": slug,
                "issue": "slide_count_below_minimum",
                "message": f"{slide_count} slide(s); expected at least {DIRECTOR_MIN_SLIDES}",
            }
        )
        return

    try:
        sidecar = _load_json_object(sidecar_path)
    except Exception as exc:
        failures.append(
            {
                "scope": "director",
                "slug": slug,
                "issue": "sidecar_load_failed",
                "message": str(exc),
            }
        )
        return

    missing_fields = [field for field in DIRECTOR_SIDECAR_FIELDS if field not in sidecar]
    if missing_fields:
        failures.append(
            {
                "scope": "director",
                "slug": slug,
                "issue": "missing_sidecar_fields",
                "message": ", ".join(missing_fields),
            }
        )
        return

    validated.append(
        {
            "slug": slug,
            "deck_path": _display_path(deck_path),
            "sidecar_path": _display_path(sidecar_path),
            "slide_count": slide_count,
            "file_size_bytes": int(deck_path.stat().st_size),
            "sidecar_metrics": _director_sidecar_metrics(sidecar),
        }
    )


def _validate_exec_rollup(
    *,
    decks_dir: Path,
    failures: list[dict[str, Any]],
) -> dict[str, Any] | None:
    deck_path = decks_dir / "Exec Rollup.pptx"
    sidecar_path = decks_dir / "Exec Rollup.json"
    if not deck_path.exists():
        failures.append(
            {
                "scope": "exec_rollup",
                "issue": "missing_deck",
                "message": f"missing {deck_path}",
            }
        )
        return None
    if not sidecar_path.exists():
        failures.append(
            {
                "scope": "exec_rollup",
                "issue": "missing_sidecar",
                "message": f"missing {sidecar_path}",
            }
        )
        return None

    try:
        slide_count = _presentation_slide_count(deck_path)
    except Exception as exc:
        failures.append(
            {
                "scope": "exec_rollup",
                "issue": "deck_load_failed",
                "message": str(exc),
            }
        )
        return None
    if slide_count < EXEC_ROLLUP_MIN_SLIDES:
        failures.append(
            {
                "scope": "exec_rollup",
                "issue": "slide_count_below_minimum",
                "message": f"{slide_count} slide(s); expected at least {EXEC_ROLLUP_MIN_SLIDES}",
            }
        )
        return None

    try:
        sidecar = _load_json_object(sidecar_path)
    except Exception as exc:
        failures.append(
            {
                "scope": "exec_rollup",
                "issue": "sidecar_load_failed",
                "message": str(exc),
            }
        )
        return None

    return {
        "deck_path": _display_path(deck_path),
        "sidecar_path": _display_path(sidecar_path),
        "slide_count": slide_count,
        "file_size_bytes": int(deck_path.stat().st_size),
        "sidecar_keys": sorted(sidecar.keys()),
    }


def _write_run_audit(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "deck_delivery_contract_audit.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    lines = [
        f"# Deck Delivery Contract Audit — {payload['run_date']}",
        "",
        f"- Status: `{payload['status']}`",
        f"- Workbooks dir: `{payload['workbooks_dir']}`",
        f"- Decks dir: `{payload['decks_dir']}`",
        f"- Expected director decks: `{payload['expected_director_count']}`",
        f"- Validated director decks: `{payload['validated_director_count']}`",
        f"- Failures: `{len(payload.get('failures') or [])}`",
        f"- Warnings: `{len(payload.get('warnings') or [])}`",
        "",
        "## Directors",
        "",
    ]
    directors = payload.get("directors") or []
    if not directors:
        lines.append("- none")
    else:
        for item in directors:
            lines.append(f"- `{item['slug']}`: `{item['slide_count']}` slide(s)")
    lines.extend(["", "## Exec Rollup", ""])
    exec_rollup = payload.get("exec_rollup")
    if not exec_rollup:
        lines.append("- none")
    else:
        lines.append(f"- `Exec Rollup`: `{exec_rollup['slide_count']}` slide(s)")
    lines.extend(["", "## Failures", ""])
    failures = payload.get("failures") or []
    if not failures:
        lines.append("- none")
    else:
        for item in failures:
            scope = item.get("slug") or item.get("scope") or "unknown"
            lines.append(
                f"- `{scope}`: `{item.get('issue', 'unknown')}` {item.get('message', '')}".strip()
            )
    lines.extend(["", "## Warnings", ""])
    warnings = payload.get("warnings") or []
    if not warnings:
        lines.append("- none")
    else:
        for item in warnings:
            lines.append(
                f"- `{item.get('issue', 'unknown')}` {item.get('message', '')}".strip()
            )
    (output_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="Explicit report date (YYYY-MM-DD).")
    parser.add_argument(
        "--workbooks-dir",
        type=Path,
        help="Directory containing per-director workbooks.",
    )
    parser.add_argument(
        "--decks-dir",
        type=Path,
        help="Directory containing per-director decks and sidecars.",
    )
    args = parser.parse_args()

    run_date = _infer_report_date(args.workbooks_dir, args.decks_dir, args.date)
    workbooks_dir = args.workbooks_dir or (ROOT / "output" / "director_live_workbooks" / run_date)
    decks_dir = args.decks_dir or (ROOT / "output" / "simcorp_director_decks" / run_date / "land-only")
    output_dir = OUTPUT_ROOT / run_date

    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    directors: list[dict[str, Any]] = []

    if not workbooks_dir.exists():
        failures.append(
            {
                "scope": "workbooks",
                "issue": "missing_workbooks_dir",
                "message": f"missing {workbooks_dir}",
            }
        )
        expected_slugs: list[str] = []
    else:
        expected_slugs = _director_slugs(workbooks_dir)
        if not expected_slugs:
            failures.append(
                {
                    "scope": "workbooks",
                    "issue": "no_workbooks_found",
                    "message": f"no director workbooks in {workbooks_dir}",
                }
            )

    if not decks_dir.exists():
        failures.append(
            {
                "scope": "decks",
                "issue": "missing_decks_dir",
                "message": f"missing {decks_dir}",
            }
        )
    else:
        for slug in expected_slugs:
            _validate_director_deck(
                slug=slug,
                decks_dir=decks_dir,
                validated=directors,
                failures=failures,
            )

        actual_deck_slugs = {
            path.stem.replace("-LAND", "")
            for path in decks_dir.glob("*-LAND.pptx")
            if not path.name.startswith("~")
        }
        for slug in sorted(actual_deck_slugs - set(expected_slugs)):
            warnings.append(
                {
                    "issue": "unexpected_director_deck",
                    "message": f"{slug}-LAND.pptx",
                }
            )

    exec_rollup = _validate_exec_rollup(decks_dir=decks_dir, failures=failures) if decks_dir.exists() else None

    payload = {
        "run_date": run_date,
        "workbooks_dir": str(workbooks_dir),
        "decks_dir": str(decks_dir),
        "status": "failed" if failures else "ok",
        "expected_director_count": len(expected_slugs),
        "validated_director_count": len(directors),
        "directors": directors,
        "exec_rollup": exec_rollup,
        "failures": failures,
        "warnings": warnings,
    }
    _write_run_audit(output_dir, payload)
    print(f"Deck delivery contract audit: {_display_path(output_dir)}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
