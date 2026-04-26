#!/usr/bin/env python3
"""Diff legacy deck sidecar payloads across run dates.

The legacy deck lane does not emit standalone fill-payload artifacts. The
deterministic JSON sidecars written next to each deck are the source-truth
payload contract for what the deck actually rendered, so this script diffs
those sidecars across monthly runs.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DECKS_ROOT = ROOT / "output" / "simcorp_director_decks"
OUTPUT_ROOT = ROOT / "output" / "deck_fill_payload_snapshot_diff"
DIRECTOR_SIDECAR_SUFFIX = "-LAND.json"
EXEC_ROLLUP_FILENAME = "Exec Rollup.json"
IGNORED_TOP_LEVEL_KEYS = {"built_at"}


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _run_dir(run_date: str) -> Path:
    return DECKS_ROOT / str(run_date)[:10] / "land-only"


def _available_run_dates() -> list[str]:
    return sorted(
        path.parent.name
        for path in DECKS_ROOT.glob("*/land-only")
        if path.is_dir()
    )


def _resolve_baseline_date(current_date: str, baseline_date: str | None = None) -> str | None:
    if baseline_date:
        return str(baseline_date)[:10]
    earlier = [
        run_date
        for run_date in _available_run_dates()
        if run_date < current_date and _run_dir(run_date).exists()
    ]
    if earlier:
        return earlier[-1]
    return None


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key not in IGNORED_TOP_LEVEL_KEYS
    }


def _diff_scalar_map(
    before_map: dict[str, Any],
    after_map: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    changes: dict[str, dict[str, Any]] = {}
    for key in sorted(set(before_map) | set(after_map)):
        before_value = before_map.get(key)
        after_value = after_map.get(key)
        if before_value == after_value:
            continue
        entry: dict[str, Any] = {"before": before_value, "after": after_value}
        if isinstance(before_value, (int, float)) and isinstance(after_value, (int, float)):
            entry["delta"] = round(float(after_value) - float(before_value), 2)
        changes[key] = entry
    return changes


def _load_director_payloads(run_dir: Path) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for path in sorted(run_dir.glob(f"*{DIRECTOR_SIDECAR_SUFFIX}")):
        slug = path.name[: -len(DIRECTOR_SIDECAR_SUFFIX)]
        payloads[slug] = {
            "payload_path": _display_path(path),
            "payload": _normalize_payload(_load_json(path)),
        }
    return payloads


def _load_exec_rollup_payload(run_dir: Path) -> dict[str, Any] | None:
    path = run_dir / EXEC_ROLLUP_FILENAME
    if not path.exists():
        return None
    return {
        "payload_path": _display_path(path),
        "payload": _normalize_payload(_load_json(path)),
    }


def _load_snapshot(run_date: str) -> dict[str, Any]:
    run_dir = _run_dir(run_date)
    if not run_dir.exists():
        raise FileNotFoundError(f"Deck sidecar directory missing: {run_dir}")
    directors = _load_director_payloads(run_dir)
    return {
        "run_date": str(run_date)[:10],
        "payload_dir": _display_path(run_dir),
        "directors": directors,
        "exec_rollup": _load_exec_rollup_payload(run_dir),
    }


def _diff_director_payloads(
    before_items: dict[str, dict[str, Any]],
    after_items: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for slug in sorted(set(before_items) | set(after_items)):
        before_item = before_items.get(slug)
        after_item = after_items.get(slug)
        if before_item is None:
            changes.append(
                {
                    "change": "added",
                    "slug": slug,
                    "payload_path_after": after_item["payload_path"],
                    "after": after_item["payload"],
                }
            )
            continue
        if after_item is None:
            changes.append(
                {
                    "change": "removed",
                    "slug": slug,
                    "payload_path_before": before_item["payload_path"],
                    "before": before_item["payload"],
                }
            )
            continue
        field_changes = _diff_scalar_map(before_item["payload"], after_item["payload"])
        if field_changes:
            changes.append(
                {
                    "change": "modified",
                    "slug": slug,
                    "payload_path_before": before_item["payload_path"],
                    "payload_path_after": after_item["payload_path"],
                    "field_changes": field_changes,
                }
            )
    return changes


def _diff_exec_rollup(
    before_item: dict[str, Any] | None,
    after_item: dict[str, Any] | None,
) -> dict[str, Any]:
    if before_item is None and after_item is None:
        return {"change": "unchanged", "scalar_changes": {}, "by_director_changes": []}
    if before_item is None:
        return {
            "change": "added",
            "payload_path_after": after_item["payload_path"],
            "after": after_item["payload"],
            "scalar_changes": {},
            "by_director_changes": [],
        }
    if after_item is None:
        return {
            "change": "removed",
            "payload_path_before": before_item["payload_path"],
            "before": before_item["payload"],
            "scalar_changes": {},
            "by_director_changes": [],
        }

    before_payload = dict(before_item["payload"])
    after_payload = dict(after_item["payload"])
    before_by_director = dict(before_payload.pop("by_director", {}) or {})
    after_by_director = dict(after_payload.pop("by_director", {}) or {})

    by_director_changes: list[dict[str, Any]] = []
    for director_name in sorted(set(before_by_director) | set(after_by_director)):
        before_director = before_by_director.get(director_name)
        after_director = after_by_director.get(director_name)
        if before_director is None:
            by_director_changes.append(
                {
                    "change": "added",
                    "director": director_name,
                    "after": after_director,
                }
            )
            continue
        if after_director is None:
            by_director_changes.append(
                {
                    "change": "removed",
                    "director": director_name,
                    "before": before_director,
                }
            )
            continue
        scalar_changes = _diff_scalar_map(
            dict(before_director),
            dict(after_director),
        )
        if scalar_changes:
            by_director_changes.append(
                {
                    "change": "modified",
                    "director": director_name,
                    "field_changes": scalar_changes,
                }
            )

    scalar_changes = _diff_scalar_map(before_payload, after_payload)
    change = "modified" if scalar_changes or by_director_changes else "unchanged"
    return {
        "change": change,
        "payload_path_before": before_item["payload_path"],
        "payload_path_after": after_item["payload_path"],
        "scalar_changes": scalar_changes,
        "by_director_changes": by_director_changes,
    }


def build_snapshot_diff(
    baseline_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> dict[str, Any]:
    director_changes = _diff_director_payloads(
        dict(baseline_snapshot.get("directors") or {}),
        dict(current_snapshot.get("directors") or {}),
    )
    exec_rollup_changes = _diff_exec_rollup(
        baseline_snapshot.get("exec_rollup"),
        current_snapshot.get("exec_rollup"),
    )
    return {
        "status": "ok",
        "baseline_run_date": str(baseline_snapshot.get("run_date") or ""),
        "current_run_date": str(current_snapshot.get("run_date") or ""),
        "payload_dirs": {
            "before": baseline_snapshot.get("payload_dir"),
            "after": current_snapshot.get("payload_dir"),
        },
        "director_payloads": {
            "count_before": len(dict(baseline_snapshot.get("directors") or {})),
            "count_after": len(dict(current_snapshot.get("directors") or {})),
            "changes": director_changes,
        },
        "exec_rollup": exec_rollup_changes,
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Deck Fill Payload Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier deck sidecar payload set was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    director_changes = list(payload["director_payloads"].get("changes") or [])
    added = [item["slug"] for item in director_changes if item.get("change") == "added"]
    removed = [item["slug"] for item in director_changes if item.get("change") == "removed"]
    modified = [item for item in director_changes if item.get("change") == "modified"]
    exec_rollup = dict(payload.get("exec_rollup") or {})
    exec_scalar_keys = sorted(dict(exec_rollup.get("scalar_changes") or {}).keys())
    exec_director_changes = list(exec_rollup.get("by_director_changes") or [])

    lines = [
        f"# Deck Fill Payload Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Status: `{payload['status']}`",
        f"- Payload dirs: `{payload['payload_dirs']['before']}` -> `{payload['payload_dirs']['after']}`",
        f"- Director payloads: `{payload['director_payloads']['count_before']}` -> `{payload['director_payloads']['count_after']}`",
        f"- Added director payloads: `{len(added)}`",
        f"- Removed director payloads: `{len(removed)}`",
        f"- Modified director payloads: `{len(modified)}`",
        f"- Exec rollup change: `{exec_rollup.get('change', 'unchanged')}`",
        f"- Exec rollup scalar fields changed: `{len(exec_scalar_keys)}`",
        f"- Exec rollup director rows changed: `{len(exec_director_changes)}`",
        "",
        "## Director Changes",
        "",
    ]

    if not director_changes:
        lines.append("- none")
    else:
        for item in director_changes:
            if item["change"] == "modified":
                changed_fields = ", ".join(sorted(item["field_changes"].keys()))
                lines.append(f"- `{item['slug']}`: `{changed_fields}`")
            elif item["change"] == "added":
                lines.append(f"- `{item['slug']}`: `added`")
            else:
                lines.append(f"- `{item['slug']}`: `removed`")

    lines.extend(["", "## Exec Rollup", ""])
    if exec_rollup.get("change") == "unchanged":
        lines.append("- none")
    else:
        if exec_scalar_keys:
            lines.append(f"- Scalar fields: `{', '.join(exec_scalar_keys)}`")
        if exec_director_changes:
            lines.append(
                "- Director rows: `"
                + ", ".join(item["director"] for item in exec_director_changes)
                + "`"
            )
        if not exec_scalar_keys and not exec_director_changes:
            lines.append(f"- Change: `{exec_rollup.get('change')}`")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current deck sidecar run date, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--baseline-date",
        default=None,
        help="Optional baseline run date. Defaults to the latest prior payload set on disk.",
    )
    args = parser.parse_args()

    current_date = str(args.current_date)[:10]
    current_dir = _run_dir(current_date)
    if not current_dir.exists():
        print(f"Current deck sidecar directory missing: {current_dir}", file=sys.stderr)
        return 1

    output_dir = OUTPUT_ROOT / current_date
    output_dir.mkdir(parents=True, exist_ok=True)

    baseline_date = _resolve_baseline_date(current_date, args.baseline_date)
    if not baseline_date:
        payload = {
            "status": "skipped",
            "reason": "baseline_not_found",
            "current_run_date": current_date,
        }
        (output_dir / "deck_fill_payload_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Deck fill payload snapshot diff: skipped (no baseline found)")
        print(f"Output: {_display_path(output_dir)}")
        return 0

    baseline_dir = _run_dir(baseline_date)
    if not baseline_dir.exists():
        print(f"Baseline deck sidecar directory missing: {baseline_dir}", file=sys.stderr)
        return 1

    payload = build_snapshot_diff(
        _load_snapshot(baseline_date),
        _load_snapshot(current_date),
    )
    (output_dir / "deck_fill_payload_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)
    print("Deck fill payload snapshot diff: ok")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {_display_path(output_dir)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
