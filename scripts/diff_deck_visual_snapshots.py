#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from PIL import Image, ImageChops, ImageDraw, ImageStat


ROOT = Path(__file__).resolve().parents[1]
DECKS_ROOT = ROOT / "output" / "simcorp_director_decks"
OUTPUT_ROOT = ROOT / "output" / "deck_visual_snapshot_diff"
SLIDES_SKILL_SCRIPTS = Path.home() / ".codex" / "skills" / "slides" / "scripts"
RENDER_SCRIPT = SLIDES_SKILL_SCRIPTS / "render_slides.py"
MONTAGE_SCRIPT = SLIDES_SKILL_SCRIPTS / "create_montage.py"


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _run_dir(run_date: str) -> Path:
    return DECKS_ROOT / str(run_date)[:10] / "land-only"


def _available_run_dates() -> list[str]:
    return sorted(path.parent.name for path in DECKS_ROOT.glob("*/land-only") if path.is_dir())


def _resolve_baseline_date(current_date: str, baseline_date: str | None = None) -> str | None:
    if baseline_date:
        return str(baseline_date)[:10]
    earlier = [run_date for run_date in _available_run_dates() if run_date < current_date]
    if earlier:
        return earlier[-1]
    return None


def _deck_index(run_dir: Path) -> dict[str, Path]:
    return {
        path.stem: path
        for path in sorted(run_dir.glob("*.pptx"))
        if not path.name.startswith("~")
    }


def _slide_number(path: Path) -> int:
    match = re.search(r"slide-(\d+)\.png$", path.name)
    if not match:
        return 0
    return int(match.group(1))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mean_channel_delta(before_path: Path, after_path: Path) -> float | None:
    with Image.open(before_path) as before_img, Image.open(after_path) as after_img:
        before_rgb = before_img.convert("RGB")
        after_rgb = after_img.convert("RGB")
        if before_rgb.size != after_rgb.size:
            return None
        diff = ImageChops.difference(before_rgb, after_rgb)
        stat = ImageStat.Stat(diff)
    return round(sum(stat.mean) / len(stat.mean), 2)


def _render_deck(deck_path: Path, output_dir: Path) -> dict[str, Any]:
    slides_dir = output_dir / "slides"
    slides_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [sys.executable, str(RENDER_SCRIPT), str(deck_path), "--output_dir", str(slides_dir)],
        check=True,
        capture_output=True,
        text=True,
    )
    slide_paths = sorted(slides_dir.glob("slide-*.png"), key=_slide_number)
    if not slide_paths:
        raise RuntimeError(f"No rendered slides produced for {deck_path.name}")
    montage_path = output_dir / "montage.png"
    subprocess.run(
        [
            sys.executable,
            str(MONTAGE_SCRIPT),
            "--input_dir",
            str(slides_dir),
            "--output_file",
            str(montage_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return {
        "slides_dir": slides_dir,
        "montage_path": montage_path,
        "slide_paths": slide_paths,
    }


def _create_comparison_montage(
    before_montage: Path,
    after_montage: Path,
    output_path: Path,
    *,
    left_label: str,
    right_label: str,
) -> None:
    with Image.open(before_montage) as left_img, Image.open(after_montage) as right_img:
        left = left_img.convert("RGB")
        right = right_img.convert("RGB")
        width = left.width + right.width
        height = max(left.height, right.height) + 48
        canvas = Image.new("RGB", (width, height), (255, 255, 255))
        draw = ImageDraw.Draw(canvas)
        canvas.paste(left, (0, 48))
        canvas.paste(right, (left.width, 48))
        draw.rectangle([(0, 0), (width, 47)], fill=(245, 245, 245))
        draw.text((16, 16), left_label, fill=(0, 0, 0))
        draw.text((left.width + 16, 16), right_label, fill=(0, 0, 0))
        draw.line([(left.width, 0), (left.width, height)], fill=(180, 180, 180), width=2)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        canvas.save(output_path)


def _render_run_snapshot(run_date: str, render_root: Path) -> dict[str, Any]:
    run_dir = _run_dir(run_date)
    if not run_dir.exists():
        raise FileNotFoundError(f"Deck directory missing: {run_dir}")
    decks: dict[str, Any] = {}
    failures: list[dict[str, Any]] = []
    for deck_name, deck_path in _deck_index(run_dir).items():
        deck_render_dir = render_root / _slugify(deck_name)
        try:
            render = _render_deck(deck_path, deck_render_dir)
        except Exception as exc:
            failures.append(
                {
                    "deck": deck_name,
                    "deck_path": _display_path(deck_path),
                    "issue": "render_failed",
                    "message": str(exc),
                }
            )
            continue
        slides: dict[str, Any] = {}
        for slide_path in render["slide_paths"]:
            slides[slide_path.name] = {
                "image_path": _display_path(slide_path),
                "sha256": _sha256(slide_path),
            }
        decks[deck_name] = {
            "deck_path": _display_path(deck_path),
            "render_dir": _display_path(deck_render_dir),
            "slides_dir": _display_path(render["slides_dir"]),
            "montage_path": _display_path(render["montage_path"]),
            "slide_count": len(render["slide_paths"]),
            "slides": slides,
        }
    return {
        "run_date": str(run_date)[:10],
        "deck_dir": _display_path(run_dir),
        "deck_count": len(decks),
        "decks": decks,
        "render_failures": failures,
    }


def _diff_decks(
    baseline_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
    comparison_root: Path,
) -> list[dict[str, Any]]:
    baseline_decks = dict(baseline_snapshot.get("decks") or {})
    current_decks = dict(current_snapshot.get("decks") or {})
    changes: list[dict[str, Any]] = []
    for deck_name in sorted(set(baseline_decks) | set(current_decks)):
        before_item = baseline_decks.get(deck_name)
        after_item = current_decks.get(deck_name)
        if before_item is None:
            changes.append(
                {
                    "change": "added",
                    "deck": deck_name,
                    "after": {
                        "deck_path": after_item.get("deck_path"),
                        "slide_count": after_item.get("slide_count"),
                        "montage_path": after_item.get("montage_path"),
                    },
                }
            )
            continue
        if after_item is None:
            changes.append(
                {
                    "change": "removed",
                    "deck": deck_name,
                    "before": {
                        "deck_path": before_item.get("deck_path"),
                        "slide_count": before_item.get("slide_count"),
                        "montage_path": before_item.get("montage_path"),
                    },
                }
            )
            continue

        before_slides = dict(before_item.get("slides") or {})
        after_slides = dict(after_item.get("slides") or {})
        slide_changes: list[dict[str, Any]] = []
        for slide_name in sorted(set(before_slides) | set(after_slides)):
            before_slide = before_slides.get(slide_name)
            after_slide = after_slides.get(slide_name)
            if before_slide is None:
                slide_changes.append(
                    {
                        "change": "added",
                        "slide": slide_name,
                        "after_path": after_slide.get("image_path"),
                    }
                )
                continue
            if after_slide is None:
                slide_changes.append(
                    {
                        "change": "removed",
                        "slide": slide_name,
                        "before_path": before_slide.get("image_path"),
                    }
                )
                continue
            if before_slide.get("sha256") == after_slide.get("sha256"):
                continue
            mean_delta = _mean_channel_delta(
                ROOT / before_slide["image_path"],
                ROOT / after_slide["image_path"],
            )
            slide_changes.append(
                {
                    "change": "modified",
                    "slide": slide_name,
                    "before_path": before_slide.get("image_path"),
                    "after_path": after_slide.get("image_path"),
                    "mean_channel_delta": mean_delta,
                }
            )

        if slide_changes or before_item.get("slide_count") != after_item.get("slide_count"):
            comparison_path = comparison_root / _slugify(deck_name) / "comparison_montage.png"
            _create_comparison_montage(
                ROOT / before_item["montage_path"],
                ROOT / after_item["montage_path"],
                comparison_path,
                left_label=f"{baseline_snapshot['run_date']} — {deck_name}",
                right_label=f"{current_snapshot['run_date']} — {deck_name}",
            )
            changes.append(
                {
                    "change": "modified",
                    "deck": deck_name,
                    "slide_count_before": before_item.get("slide_count"),
                    "slide_count_after": after_item.get("slide_count"),
                    "slide_changes": slide_changes,
                    "baseline_montage_path": before_item.get("montage_path"),
                    "current_montage_path": after_item.get("montage_path"),
                    "comparison_montage_path": _display_path(comparison_path),
                }
            )
    return changes


def build_snapshot_diff(
    baseline_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
    comparison_root: Path,
) -> dict[str, Any]:
    deck_changes = _diff_decks(baseline_snapshot, current_snapshot, comparison_root)
    added = sum(1 for item in deck_changes if item.get("change") == "added")
    removed = sum(1 for item in deck_changes if item.get("change") == "removed")
    modified = [item for item in deck_changes if item.get("change") == "modified"]
    changed_slides = sum(len(item.get("slide_changes") or []) for item in modified)
    failure_count = len(list(baseline_snapshot.get("render_failures") or [])) + len(
        list(current_snapshot.get("render_failures") or [])
    )
    return {
        "status": "failed" if failure_count else "ok",
        "baseline_run_date": str(baseline_snapshot.get("run_date") or ""),
        "current_run_date": str(current_snapshot.get("run_date") or ""),
        "visual_diff": {
            "deck_count_before": baseline_snapshot.get("deck_count"),
            "deck_count_after": current_snapshot.get("deck_count"),
            "render_failures_before": list(baseline_snapshot.get("render_failures") or []),
            "render_failures_after": list(current_snapshot.get("render_failures") or []),
            "added_decks": added,
            "removed_decks": removed,
            "modified_decks": len(modified),
            "changed_slides": changed_slides,
            "deck_changes": deck_changes,
        },
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Deck Visual Snapshot Diff — {payload['current_run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
            "",
            "- No earlier deck set was available to diff against.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    visual_diff = payload["visual_diff"]
    lines = [
        f"# Deck Visual Snapshot Diff — {payload['baseline_run_date']} -> {payload['current_run_date']}",
        "",
        f"- Status: `{payload['status']}`",
        f"- Deck count: `{visual_diff['deck_count_before']}` -> `{visual_diff['deck_count_after']}`",
        f"- Added decks: `{visual_diff['added_decks']}`",
        f"- Removed decks: `{visual_diff['removed_decks']}`",
        f"- Modified decks: `{visual_diff['modified_decks']}`",
        f"- Changed slides: `{visual_diff['changed_slides']}`",
        f"- Render failures: `{len(visual_diff['render_failures_before'])}` -> `{len(visual_diff['render_failures_after'])}`",
        "",
        "## Modified Decks",
        "",
    ]
    modified = [item for item in visual_diff["deck_changes"] if item.get("change") == "modified"]
    if not modified:
        lines.append("- none")
    else:
        for item in modified:
            lines.append(
                f"- `{item['deck']}`: `{len(item.get('slide_changes') or [])}` changed slide(s), "
                f"comparison `{item['comparison_montage_path']}`"
            )

    if visual_diff["render_failures_before"] or visual_diff["render_failures_after"]:
        lines.extend(["", "## Render Failures", ""])
        for item in visual_diff["render_failures_before"]:
            lines.append(f"- baseline `{item['deck']}`: `{item['message']}`")
        for item in visual_diff["render_failures_after"]:
            lines.append(f"- current `{item['deck']}`: `{item['message']}`")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Current deck run date, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--baseline-date",
        default=None,
        help="Optional baseline run date. Defaults to the latest prior deck set on disk.",
    )
    args = parser.parse_args()

    current_date = str(args.current_date)[:10]
    current_dir = _run_dir(current_date)
    if not current_dir.exists():
        print(f"Current deck directory missing: {current_dir}", file=sys.stderr)
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
        (output_dir / "deck_visual_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print("Deck visual snapshot diff: skipped (no baseline found)")
        print(f"Output: {_display_path(output_dir)}")
        return 0

    baseline_dir = _run_dir(baseline_date)
    if not baseline_dir.exists():
        print(f"Baseline deck directory missing: {baseline_dir}", file=sys.stderr)
        return 1

    try:
        baseline_snapshot = _render_run_snapshot(
            baseline_date,
            output_dir / "renders" / baseline_date,
        )
        current_snapshot = _render_run_snapshot(
            current_date,
            output_dir / "renders" / current_date,
        )
        payload = build_snapshot_diff(
            baseline_snapshot,
            current_snapshot,
            output_dir / "comparisons",
        )
    except Exception as exc:
        payload = {
            "status": "failed",
            "baseline_run_date": baseline_date,
            "current_run_date": current_date,
            "visual_diff": {
                "deck_count_before": 0,
                "deck_count_after": 0,
                "render_failures_before": [],
                "render_failures_after": [],
                "added_decks": 0,
                "removed_decks": 0,
                "modified_decks": 0,
                "changed_slides": 0,
                "deck_changes": [],
            },
            "error": str(exc),
        }
        (output_dir / "deck_visual_snapshot_diff.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_summary(output_dir / "summary.md", payload)
        print(f"Deck visual snapshot diff failed: {exc}", file=sys.stderr)
        print(f"Output: {_display_path(output_dir)}")
        return 1

    (output_dir / "deck_visual_snapshot_diff.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)
    print(f"Deck visual snapshot diff: {payload['status']}")
    print(f"Baseline: {baseline_date}")
    print(f"Output: {_display_path(output_dir)}")
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
