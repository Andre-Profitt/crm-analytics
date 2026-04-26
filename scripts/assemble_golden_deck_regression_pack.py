#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "golden_deck_regression_pack.json"
OUTPUT_ROOT = ROOT / "output" / "golden_deck_regression_pack"
VISUAL_DIFF_ROOT = ROOT / "output" / "deck_visual_snapshot_diff"
FILL_DIFF_ROOT = ROOT / "output" / "deck_fill_payload_snapshot_diff"
DECKS_ROOT = ROOT / "output" / "simcorp_director_decks"
MASTER_BUILDER_ROOT = ROOT / "output" / "sales_director_monthly_master_builder"
SIDECAR_FIELDS = [
    "open_land_deals",
    "open_land_arr",
    "approved_2026",
    "conditionally_approved",
    "missing_stage3",
]


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _visual_diff_path(run_date: str) -> Path:
    return VISUAL_DIFF_ROOT / str(run_date)[:10] / "deck_visual_snapshot_diff.json"


def _fill_diff_path(run_date: str) -> Path:
    return FILL_DIFF_ROOT / str(run_date)[:10] / "deck_fill_payload_snapshot_diff.json"


def _deck_sidecar_path(run_date: str, slug: str) -> Path:
    return DECKS_ROOT / str(run_date)[:10] / "land-only" / f"{slug}-LAND.json"


def _visual_index(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    visual_diff = dict(payload.get("visual_diff") or {})
    for item in visual_diff.get("deck_changes") or []:
        deck_name = str(item.get("deck") or "").strip()
        if deck_name:
            index[deck_name] = item
    return index


def _fill_index(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    director_payloads = dict(payload.get("director_payloads") or {})
    for item in director_payloads.get("changes") or []:
        slug = str(item.get("slug") or "").strip()
        if slug:
            index[slug] = item
    return index


def _latest_modular_fill_payload(slug: str) -> Path | None:
    latest_path: Path | None = None
    latest_run: tuple[str, str] | None = None
    for path in MASTER_BUILDER_ROOT.glob(f"*/*/{slug}/validated_bridge/powerpoint-fill-payload.json"):
        snapshot_date = path.parts[-5]
        run_id = path.parts[-4]
        key = (snapshot_date, run_id)
        if latest_run is None or key > latest_run:
            latest_run = key
            latest_path = path
    return latest_path


def _extract_modular_omitted_metric(slug: str) -> dict[str, Any] | None:
    payload_path = _latest_modular_fill_payload(slug)
    if payload_path is None:
        return None
    payload = _load_json(payload_path)
    for slide in payload.get("slides") or []:
        slots = dict(slide.get("slots") or {})
        for key, value in slots.items():
            if key.endswith("_omitted_arr"):
                return {
                    "slot": key,
                    "value": value,
                    "source_payload_path": _display_path(payload_path),
                }
    return None


def _copy_montage(source: Path | None, output_dir: Path, role: str) -> str | None:
    if source is None or not source.exists():
        return None
    destination = output_dir / f"{role}-comparison_montage.png"
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return _display_path(destination)


def _build_overview_montage(items: list[dict[str, Any]], output_path: Path) -> str | None:
    montage_paths = [
        ROOT / item["visual_change"]["comparison_montage_path"]
        for item in items
        if dict(item.get("visual_change") or {}).get("comparison_montage_path")
    ]
    if not montage_paths:
        return None

    images = [Image.open(path).convert("RGB") for path in montage_paths]
    try:
        label_height = 36
        max_width = max(image.width for image in images)
        total_height = sum(image.height + label_height for image in images) + 12 * (len(images) + 1)
        canvas = Image.new("RGB", (max_width + 24, total_height), (255, 255, 255))
        draw = ImageDraw.Draw(canvas)
        y = 12
        for item, image in zip(items, images, strict=False):
            draw.rectangle([(0, y), (canvas.width, y + label_height)], fill=(245, 245, 245))
            label = f"{item['role']}: {item['director']} ({item['slug']})"
            draw.text((12, y + 10), label, fill=(0, 0, 0))
            y += label_height
            canvas.paste(image, (12, y))
            y += image.height + 12
        output_path.parent.mkdir(parents=True, exist_ok=True)
        canvas.save(output_path)
        return _display_path(output_path)
    finally:
        for image in images:
            image.close()


def _load_sidecar_metrics(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    return {
        "director": payload.get("director"),
        "territory": payload.get("territory"),
        **{field: payload.get(field) for field in SIDECAR_FIELDS},
    }


def assemble_pack(
    *,
    run_date: str,
    visual_diff: dict[str, Any],
    fill_diff: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    config = _load_json(CONFIG_PATH)
    visual_index = _visual_index(visual_diff)
    fill_index = _fill_index(fill_diff)
    baseline_date = str(visual_diff.get("baseline_run_date") or "")
    items: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for role_cfg in config.get("roles") or []:
        role = str(role_cfg.get("role") or "").strip()
        slug = str(role_cfg.get("slug") or "").strip()
        deck_name = f"{slug}-LAND"
        current_sidecar_path = _deck_sidecar_path(run_date, slug)
        baseline_sidecar_path = _deck_sidecar_path(baseline_date, slug) if baseline_date else None
        if not role or not slug:
            failures.append({"issue": "invalid_config_entry", "entry": role_cfg})
            continue
        if not current_sidecar_path.exists():
            failures.append(
                {
                    "role": role,
                    "slug": slug,
                    "issue": "missing_current_sidecar",
                    "message": str(current_sidecar_path),
                }
            )
            continue

        visual_item = visual_index.get(deck_name, {})
        fill_item = fill_index.get(slug, {})
        copied_montage_path = _copy_montage(
            ROOT / visual_item["comparison_montage_path"]
            if visual_item.get("comparison_montage_path")
            else None,
            output_dir / "selected_montages",
            role,
        )
        current_metrics = _load_sidecar_metrics(current_sidecar_path)
        baseline_metrics = (
            _load_sidecar_metrics(baseline_sidecar_path)
            if baseline_sidecar_path and baseline_sidecar_path.exists()
            else None
        )
        modular_omitted = _extract_modular_omitted_metric(slug)

        items.append(
            {
                "role": role,
                "slug": slug,
                "director": current_metrics.get("director") or slug,
                "territory": current_metrics.get("territory"),
                "selection_basis": role_cfg.get("selection_basis"),
                "current_sidecar_metrics": current_metrics,
                "baseline_sidecar_metrics": baseline_metrics,
                "modular_omitted_metric": modular_omitted,
                "visual_change": {
                    "change": visual_item.get("change", "unchanged"),
                    "slide_count_before": visual_item.get("slide_count_before"),
                    "slide_count_after": visual_item.get("slide_count_after"),
                    "changed_slide_count": len(list(visual_item.get("slide_changes") or [])),
                    "comparison_montage_path": copied_montage_path,
                    "raw_comparison_montage_path": visual_item.get("comparison_montage_path"),
                },
                "fill_payload_change": {
                    "change": fill_item.get("change", "unchanged"),
                    "changed_fields": sorted(
                        dict(fill_item.get("field_changes") or {}).keys()
                    ),
                },
            }
        )

    overview_montage_path = _build_overview_montage(
        items,
        output_dir / "golden_pack_overview.png",
    )
    status = "failed" if failures else "ok"
    return {
        "status": status,
        "run_date": run_date,
        "baseline_run_date": baseline_date,
        "config_path": _display_path(CONFIG_PATH),
        "overview_montage_path": overview_montage_path,
        "items": items,
        "failures": failures,
    }


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    if payload.get("status") == "skipped":
        lines = [
            f"# Golden Deck Regression Pack — {payload['run_date']}",
            "",
            f"- Status: `{payload['status']}`",
            f"- Reason: `{payload['reason']}`",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    lines = [
        f"# Golden Deck Regression Pack — {payload['run_date']}",
        "",
        f"- Status: `{payload['status']}`",
        f"- Baseline: `{payload['baseline_run_date']}`",
        f"- Config: `{payload['config_path']}`",
        f"- Directors: `{len(payload['items'])}`",
        f"- Overview montage: `{payload.get('overview_montage_path') or 'none'}`",
        "",
    ]
    for item in payload["items"]:
        current_metrics = dict(item.get("current_sidecar_metrics") or {})
        visual_change = dict(item.get("visual_change") or {})
        fill_change = dict(item.get("fill_payload_change") or {})
        modular_omitted = item.get("modular_omitted_metric") or {}
        lines.extend(
            [
                f"## {item['role']}",
                "",
                f"- Director: `{item['director']}` ({item['slug']})",
                f"- Territory: `{item.get('territory') or 'unknown'}`",
                f"- Selection basis: {item.get('selection_basis')}",
                f"- Legacy sidecar: `open_land_deals={current_metrics.get('open_land_deals')}`, "
                f"`approved_2026={current_metrics.get('approved_2026')}`, "
                f"`conditionally_approved={current_metrics.get('conditionally_approved')}`, "
                f"`missing_stage3={current_metrics.get('missing_stage3')}`",
                f"- Modular omitted metric: `{modular_omitted.get('value') or 'n/a'}`",
                f"- Visual drift: `{visual_change.get('changed_slide_count', 0)}` changed slide(s)",
                f"- Visual montage: `{visual_change.get('comparison_montage_path') or 'n/a'}`",
                f"- Fill-payload fields changed: `{', '.join(fill_change.get('changed_fields') or []) or 'none'}`",
                "",
            ]
        )
    if payload["failures"]:
        lines.extend(["## Failures", ""])
        for failure in payload["failures"]:
            lines.append(f"- `{failure.get('issue')}`: `{failure.get('message') or failure}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date, YYYY-MM-DD. Defaults to today.",
    )
    args = parser.parse_args()

    run_date = str(args.date)[:10]
    visual_diff_path = _visual_diff_path(run_date)
    fill_diff_path = _fill_diff_path(run_date)
    output_dir = OUTPUT_ROOT / run_date
    output_dir.mkdir(parents=True, exist_ok=True)

    if not visual_diff_path.exists():
        print(f"Visual diff missing: {visual_diff_path}", file=sys.stderr)
        return 1
    if not fill_diff_path.exists():
        print(f"Fill diff missing: {fill_diff_path}", file=sys.stderr)
        return 1

    visual_diff = _load_json(visual_diff_path)
    fill_diff = _load_json(fill_diff_path)
    if visual_diff.get("status") == "skipped":
        payload = {
            "status": "skipped",
            "reason": "visual_diff_baseline_not_found",
            "run_date": run_date,
        }
    elif fill_diff.get("status") == "skipped":
        payload = {
            "status": "skipped",
            "reason": "fill_diff_baseline_not_found",
            "run_date": run_date,
        }
    else:
        payload = assemble_pack(
            run_date=run_date,
            visual_diff=visual_diff,
            fill_diff=fill_diff,
            output_dir=output_dir,
        )

    (output_dir / "golden_deck_regression_pack.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_summary(output_dir / "summary.md", payload)
    print(f"Golden deck regression pack: {payload['status']}")
    print(f"Output: {_display_path(output_dir)}")
    return 0 if payload["status"] in {"ok", "skipped"} else 1


if __name__ == "__main__":
    sys.exit(main())
