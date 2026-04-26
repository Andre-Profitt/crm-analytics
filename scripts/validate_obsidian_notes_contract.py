#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from generate_obsidian_notes import DIRECTORS
except ModuleNotFoundError:  # pragma: no cover
    from scripts.generate_obsidian_notes import DIRECTORS


ROOT = Path(__file__).resolve().parents[1]
VAULT = ROOT / "obsidian"
WORKBOOKS_ROOT = ROOT / "output" / "director_live_workbooks"
OUTPUT_ROOT = ROOT / "output" / "obsidian_notes_contract"


def _slug(name: str) -> str:
    return name.lower().replace(" ", "-").replace(",", "").replace("'", "")


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _load_snapshot_history(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _snapshot_entry(payload: dict[str, Any], run_date: str) -> dict[str, Any] | None:
    snapshots = list(payload.get("snapshots") or [])
    for item in snapshots:
        if str(item.get("run_date") or "")[:10] == run_date:
            return item
    return None


def _validate_run(
    *,
    run_date: str,
    workbooks_dir: Path,
) -> dict[str, Any]:
    month_key = run_date[:7]
    month_dir = VAULT / "Monthly" / month_key
    monthly_summary_path = month_dir / "README.md"
    snapshot_history_path = VAULT / "snapshot_history.json"
    validated: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    if not workbooks_dir.exists():
        failures.append(
            {
                "director": None,
                "issue": "missing_workbooks_dir",
                "message": f"missing {workbooks_dir}",
            }
        )
        return {
            "run_date": run_date,
            "period": month_key,
            "status": "failed",
            "validated": validated,
            "failures": failures,
            "warnings": warnings,
            "monthly_summary_path": _display_path(monthly_summary_path),
            "snapshot_history_path": _display_path(snapshot_history_path),
            "expected_director_count": 0,
        }

    if not month_dir.exists():
        failures.append(
            {
                "director": None,
                "issue": "missing_month_dir",
                "message": f"missing {month_dir}",
            }
        )

    if not monthly_summary_path.exists():
        failures.append(
            {
                "director": None,
                "issue": "missing_monthly_summary",
                "message": f"missing {monthly_summary_path}",
            }
        )

    snapshot_payload: dict[str, Any] | None = None
    snapshot_run: dict[str, Any] | None = None
    if not snapshot_history_path.exists():
        failures.append(
            {
                "director": None,
                "issue": "missing_snapshot_history",
                "message": f"missing {snapshot_history_path}",
            }
        )
    else:
        try:
            snapshot_payload = _load_snapshot_history(snapshot_history_path)
            snapshot_run = _snapshot_entry(snapshot_payload, run_date)
            if snapshot_run is None:
                failures.append(
                    {
                        "director": None,
                        "issue": "missing_snapshot_run",
                        "message": f"run_date {run_date} not found in snapshot_history.json",
                    }
                )
        except Exception as exc:
            failures.append(
                {
                    "director": None,
                    "issue": "snapshot_history_parse_failed",
                    "message": str(exc),
                }
            )

    expected_directors = []
    for director, territory, filename in DIRECTORS:
        if (workbooks_dir / filename).exists():
            expected_directors.append((director, territory))

    if not expected_directors:
        failures.append(
            {
                "director": None,
                "issue": "no_workbooks_found",
                "message": f"no director workbooks found under {workbooks_dir}",
            }
        )

    snapshot_directors = snapshot_run.get("directors") if isinstance(snapshot_run, dict) else {}
    if not isinstance(snapshot_directors, dict):
        snapshot_directors = {}

    for director, territory in expected_directors:
        slug = _slug(director)
        auto_path = month_dir / f"{slug}.auto.md"
        notes_path = month_dir / f"{slug}.notes.md"
        standing_path = VAULT / "Directors" / f"{slug}.md"
        director_failures = []
        for issue, path in [
            ("missing_auto_note", auto_path),
            ("missing_commentary_note", notes_path),
            ("missing_standing_page", standing_path),
        ]:
            if not path.exists():
                director_failures.append(
                    {
                        "director": director,
                        "issue": issue,
                        "message": f"missing {path}",
                    }
                )
        snapshot_director = snapshot_directors.get(director)
        if snapshot_run is not None and snapshot_director is None:
            director_failures.append(
                {
                    "director": director,
                    "issue": "missing_snapshot_director",
                    "message": f"{director} missing from snapshot history for {run_date}",
                }
            )
        elif isinstance(snapshot_director, dict):
            if str(snapshot_director.get("territory") or "") != territory:
                director_failures.append(
                    {
                        "director": director,
                        "issue": "snapshot_territory_mismatch",
                        "message": (
                            f"{snapshot_director.get('territory')} != {territory}"
                        ),
                    }
                )

        if director_failures:
            failures.extend(director_failures)
            continue

        validated.append(
            {
                "director": director,
                "slug": slug,
                "territory": territory,
                "auto_note_path": _display_path(auto_path),
                "commentary_note_path": _display_path(notes_path),
                "standing_page_path": _display_path(standing_path),
                "snapshot_history_present": director in snapshot_directors,
            }
        )

    return {
        "run_date": run_date,
        "period": month_key,
        "status": "failed" if failures else "ok",
        "validated": validated,
        "failures": failures,
        "warnings": warnings,
        "monthly_summary_path": _display_path(monthly_summary_path),
        "snapshot_history_path": _display_path(snapshot_history_path),
        "expected_director_count": len(expected_directors),
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        f"# Obsidian Notes Contract — {payload['run_date']}",
        "",
        f"- Status: `{payload['status']}`",
        f"- Period: `{payload['period']}`",
        f"- Expected directors: `{payload['expected_director_count']}`",
        f"- Validated directors: `{len(payload['validated'])}`",
        f"- Failures: `{len(payload['failures'])}`",
        f"- Warnings: `{len(payload['warnings'])}`",
        "",
        "## Outputs",
        "",
        f"- Monthly summary: `{payload['monthly_summary_path']}`",
        f"- Snapshot history: `{payload['snapshot_history_path']}`",
    ]
    if payload["failures"]:
        lines.extend(["", "## Failures", ""])
        for item in payload["failures"]:
            owner = item.get("director") or "run"
            lines.append(f"- `{owner}`: `{item['issue']}` — {item['message']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--workbooks-dir",
        type=Path,
        default=None,
        help="Defaults to output/director_live_workbooks/<date>.",
    )
    args = parser.parse_args()

    run_date = str(args.date)[:10]
    workbooks_dir = args.workbooks_dir or (WORKBOOKS_ROOT / run_date)
    output_dir = OUTPUT_ROOT / run_date
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = _validate_run(run_date=run_date, workbooks_dir=workbooks_dir)
    payload_path = output_dir / "obsidian_notes_contract_audit.json"
    summary_path = output_dir / "summary.md"
    payload_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_summary(summary_path, payload)

    print(f"Obsidian notes contract: {payload['status']}")
    print(f"Output: {output_dir}")
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
