#!/usr/bin/env python3
"""Track F / F6 — golden visual regression validator.

Renders the produced PPTX to per-slide PNGs, crops each slide to the
frozen regions declared in config/deck_visual_regions.yaml, hashes the
cropped pixels, and compares to a golden baseline manifest.

Two modes:

  --mode capture
      Render the input PPTX, hash all frozen regions, and overwrite the
      baseline manifest. Use to seed the baseline from a known-good
      build, or to explicitly re-bless it after an intentional brand
      change.

  --mode verify  (default)
      Render and hash, then compare against the committed baseline.
      Mismatch is a blocker (frozen brand region drifted).

Baseline location:
  tests/fixtures/track_f/golden_baseline/manifest.json

Mismatches print which slide + region drifted. The crop bytes are
hashed via SHA-256; tolerance is 0 (frozen regions are bit-exact).
A future enhancement could add perceptual-hash tolerance for
anti-aliasing variance across renderers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform import deck_contract  # noqa: E402
from scripts.render_deck_to_images import render_to_pngs  # noqa: E402


REPORT_SCHEMA_VERSION = "monthly_platform.deck_visual_regression_report.v1"
DEFAULT_REGIONS_PATH = REPO_ROOT / "config" / "deck_visual_regions.yaml"
DEFAULT_BASELINE_PATH = (
    REPO_ROOT / "tests" / "fixtures" / "track_f" / "golden_baseline" / "manifest.json"
)


def _load_regions(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _slide_regions(
    regions: dict[str, Any], slide_id: str | None
) -> list[dict[str, Any]]:
    if slide_id and slide_id in (regions.get("slides") or {}):
        return regions["slides"][slide_id].get("frozen_regions", []) or []
    return regions.get("defaults", {}).get("frozen_regions", []) or []


def _hash_region(png_path: Path, bbox: list[float]) -> tuple[str, dict[str, int]]:
    """Crop the PNG to the normalised bbox and return SHA-256 of the
    cropped raw bytes. Returns (hash_hex, pixel_geometry_dict)."""
    img = Image.open(png_path)
    w, h = img.size
    left = int(round(bbox[0] * w))
    top = int(round(bbox[1] * h))
    right = int(round((bbox[0] + bbox[2]) * w))
    bottom = int(round((bbox[1] + bbox[3]) * h))
    crop = img.crop((left, top, right, bottom))
    # Normalise mode for hash stability across renders.
    if crop.mode != "RGB":
        crop = crop.convert("RGB")
    h_obj = hashlib.sha256(crop.tobytes())
    return h_obj.hexdigest(), {
        "left": left,
        "top": top,
        "right": right,
        "bottom": bottom,
        "width": right - left,
        "height": bottom - top,
    }


def _slide_id_for_number(
    contract_obj: deck_contract.DeckContract, snum: int
) -> str | None:
    profile = contract_obj.director_monthly
    for s in profile.get("slides", []):
        if int(s["slide_number"]) == snum:
            return s["id"]
    return None


def hash_deck(
    pptx: Path,
    *,
    contract: deck_contract.DeckContract,
    regions: dict[str, Any],
    dpi: int = 100,
) -> dict[str, Any]:
    """Render + hash. Returns {slides: [{slide_number, slide_id,
    regions: [{id, kind, bbox, hash, geometry}]}]}."""
    with tempfile.TemporaryDirectory(prefix="track_f_visual_") as td:
        tdp = Path(td)
        pngs = render_to_pngs(pptx, tdp, dpi=dpi)
        out_slides: list[dict[str, Any]] = []
        for snum, png in enumerate(pngs, start=1):
            sid = _slide_id_for_number(contract, snum)
            slide_regions = _slide_regions(regions, sid)
            region_hashes = []
            for r in slide_regions:
                h, geom = _hash_region(png, r["bbox"])
                region_hashes.append(
                    {
                        "id": r["id"],
                        "kind": r.get("kind", "frozen"),
                        "bbox": r["bbox"],
                        "hash": h,
                        "pixel_geometry": geom,
                    }
                )
            out_slides.append(
                {
                    "slide_number": snum,
                    "slide_id": sid,
                    "regions": region_hashes,
                }
            )
    return {"slides": out_slides}


def write_baseline(
    snapshot: dict[str, Any], baseline_path: Path, *, pptx_path: Path, dpi: int
) -> None:
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "monthly_platform.deck_visual_regression_baseline.v1",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "captured_from_pptx": str(pptx_path),
        "dpi": dpi,
        "slides": snapshot["slides"],
    }
    baseline_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def verify_against_baseline(
    snapshot: dict[str, Any],
    baseline_path: Path,
) -> dict[str, Any]:
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    base_by_num = {int(s["slide_number"]): s for s in baseline.get("slides", [])}

    findings: list[dict[str, Any]] = []
    slide_results: list[dict[str, Any]] = []

    for actual in snapshot["slides"]:
        snum = actual["slide_number"]
        sid = actual.get("slide_id")
        base = base_by_num.get(snum)
        if base is None:
            findings.append(
                {
                    "severity": "blocker",
                    "code": "baseline_slide_missing",
                    "path": f"slides[{snum}]",
                    "message": f"slide {snum} ({sid}) not in baseline",
                }
            )
            slide_results.append(
                {
                    "slide_number": snum,
                    "slide_id": sid,
                    "status": "fail",
                    "regions": [],
                }
            )
            continue
        base_by_id = {r["id"]: r for r in base.get("regions", [])}

        region_results: list[dict[str, Any]] = []
        slide_status = "pass"
        for actual_r in actual.get("regions", []):
            base_r = base_by_id.get(actual_r["id"])
            if base_r is None:
                findings.append(
                    {
                        "severity": "blocker",
                        "code": "baseline_region_missing",
                        "path": f"slides[{snum}].regions[{actual_r['id']}]",
                        "message": f"region {actual_r['id']!r} not in baseline for slide {snum}",
                    }
                )
                region_results.append(
                    {
                        "id": actual_r["id"],
                        "status": "fail",
                        "actual_hash": actual_r["hash"],
                    }
                )
                slide_status = "fail"
                continue
            match = actual_r["hash"] == base_r["hash"]
            region_results.append(
                {
                    "id": actual_r["id"],
                    "status": "pass" if match else "fail",
                    "actual_hash": actual_r["hash"],
                    "baseline_hash": base_r["hash"],
                }
            )
            if not match:
                findings.append(
                    {
                        "severity": "blocker",
                        "code": "frozen_region_drift",
                        "path": f"slides[{snum}].regions[{actual_r['id']}]",
                        "message": (
                            f"frozen region {actual_r['id']!r} on slide {snum} "
                            f"({sid}) drifted: actual={actual_r['hash'][:12]}..., "
                            f"baseline={base_r['hash'][:12]}..."
                        ),
                    }
                )
                slide_status = "fail"
        slide_results.append(
            {
                "slide_number": snum,
                "slide_id": sid,
                "status": slide_status,
                "regions": region_results,
            }
        )

    blockers = [f for f in findings if f["severity"] == "blocker"]
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_path": str(baseline_path),
        "captured_at": baseline.get("captured_at"),
        "status": "pass" if not blockers else "fail",
        "blocker_count": len(blockers),
        "warning_count": 0,
        "slide_count": len(snapshot["slides"]),
        "baseline_slide_count": len(baseline.get("slides", [])),
        "slides": slide_results,
        "findings": findings,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Visual regression validator (Track F F6)."
    )
    parser.add_argument("--pptx", required=True, type=Path)
    parser.add_argument("--mode", choices=["capture", "verify"], default="verify")
    parser.add_argument("--regions", type=Path, default=DEFAULT_REGIONS_PATH)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE_PATH)
    parser.add_argument("--contract", default=None)
    parser.add_argument("--dpi", type=int, default=100)
    parser.add_argument("--report-out", type=Path, default=None)
    parser.add_argument("--show-findings", action="store_true")
    args = parser.parse_args(argv)

    if not args.pptx.exists():
        print(f"ERROR: pptx not found: {args.pptx}", file=sys.stderr)
        return 2
    if not args.regions.exists():
        print(f"ERROR: regions config not found: {args.regions}", file=sys.stderr)
        return 2

    contract = deck_contract.load(args.contract)
    regions = _load_regions(args.regions)
    snapshot = hash_deck(args.pptx, contract=contract, regions=regions, dpi=args.dpi)

    if args.mode == "capture":
        write_baseline(snapshot, args.baseline, pptx_path=args.pptx, dpi=args.dpi)
        print(f"baseline written: {args.baseline}")
        slide_count = len(snapshot["slides"])
        region_count = sum(len(s["regions"]) for s in snapshot["slides"])
        print(
            f"deck_visual_baseline: captured ({slide_count} slides, {region_count} regions)"
        )
        return 0

    # verify
    if not args.baseline.exists():
        print(f"ERROR: baseline not found at {args.baseline}", file=sys.stderr)
        print("Run with --mode capture to seed it.", file=sys.stderr)
        return 2

    report = verify_against_baseline(snapshot, args.baseline)
    if args.report_out:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )
        print(f"report: {args.report_out}")
    if args.show_findings or report["status"] == "fail":
        for f in report["findings"]:
            print(f"[{f['severity']}] {f['code']} {f['path']}: {f['message']}")
    print(
        f"deck_visual_regression: {report['status']} "
        f"(blockers={report['blocker_count']} slides={report['slide_count']}/{report['baseline_slide_count']})"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
